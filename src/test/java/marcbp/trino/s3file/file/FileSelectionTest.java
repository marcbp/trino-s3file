package marcbp.trino.s3file.file;

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.txt.TextTableFunction;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FileSelectionTest {
    @Test
    void paginatedListingFiltersSortsAndPinsObjects() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.maxGlobFiles()).thenReturn(10_000);
        var a = listed("logs/a.jsonl", 11);
        var b = listed("logs/nested/b.jsonl", 7);
        when(client.listObjects("bucket", "logs/", true, Optional.empty())).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(b, listed("logs/ignored.txt", 5)), List.of(), Optional.of("next")));
        when(client.listObjects("bucket", "logs/", true, Optional.of("next"))).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(a, b), List.of(), Optional.empty()));

        var selection = FileSelection.resolve(client, "s3://bucket/logs/{,**/}*.jsonl", Map.of());
        assertEquals(List.of(a.path(), b.path()), selection.objects().stream().map(S3ObjectRef::path).toList());
        assertEquals("$path", selection.pathColumn());
        assertEquals(Optional.of("etag-logs/a.jsonl"), selection.objects().getFirst().eTagRef());
        verify(client, never()).getObjectMetadata(anyString());

        var handle = new TextTableFunction.Handle(selection, new ScanSettings(4, new PageSettings(10, 1024), "UTF-8"),
                new TextTableFunction.TextOptions("\n"));
        var splits = handle.createSplits(true);
        assertEquals(5, splits.size());
        assertEquals(List.of(0L, 4L, 8L, 0L, 4L), splits.stream().map(FileSplit::startOffset).toList());
        assertEquals(List.of(a.path(), a.path(), a.path(), b.path(), b.path()), splits.stream().map(s -> s.object().path()).toList());
        assertEquals(2, handle.createSplits(false).size());
    }

    @Test
    void inferenceSkipsEmptyObjectsAndStopsAtFirstUsableSchema() throws Exception {
        var client = mock(S3ClientBuilder.SessionClient.class);
        var empty = new S3ObjectRef("s3://bucket/a", 0, "a", null);
        var noRows = new S3ObjectRef("s3://bucket/b", 3, "b", null);
        var reference = new S3ObjectRef("s3://bucket/c", 10, "c", null);
        var later = new S3ObjectRef("s3://bucket/d", 10, "d", null);
        when(client.openReader(noRows.path(), StandardCharsets.UTF_8, Optional.empty(), Optional.of("b")))
                .thenReturn(new BufferedReader(new StringReader("")));
        when(client.openReader(reference.path(), StandardCharsets.UTF_8, Optional.empty(), Optional.of("c")))
                .thenReturn(new BufferedReader(new StringReader("schema")));
        var selection = new FileSelection(List.of(empty, noRows, reference, later), "");
        assertEquals("schema", selection.infer(client, StandardCharsets.UTF_8, (reader, object) -> reader.readLine()));
        verify(client, never()).openReader(eq(later.path()), any(), any(), any());
        verify(client, never()).openReader(eq(empty.path()), any(), any(), any());
    }

    @Test
    void malformedReferenceIsNotSilentlyReplacedByAnotherFile() throws Exception {
        var client = mock(S3ClientBuilder.SessionClient.class);
        var first = new S3ObjectRef("s3://bucket/a", 10, null, null);
        var later = new S3ObjectRef("s3://bucket/b", 10, null, null);
        when(client.openReader(first.path(), StandardCharsets.UTF_8, Optional.empty(), Optional.empty()))
                .thenReturn(new BufferedReader(new StringReader("malformed")));
        var selection = new FileSelection(List.of(first, later), "");
        assertThrows(IllegalArgumentException.class, () -> selection.infer(client, StandardCharsets.UTF_8,
                (reader, object) -> { throw new IllegalArgumentException("Invalid record"); }));
        verify(client, never()).openReader(eq(later.path()), any(), any(), any());
    }

    @Test
    void exactPathAndExplicitProvenanceDoNotRequireListing() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.getObjectMetadata("s3://bucket/file.txt"))
                .thenReturn(new S3ClientBuilder.ObjectMetadata(10, Optional.empty(), Optional.of("version")));
        var selection = FileSelection.resolve(client, "s3://bucket/file.txt", Map.of(
                "PATH_COLUMN", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("source"))));
        assertEquals("source", selection.pathColumn());
        assertEquals(Optional.of("version"), selection.objects().getFirst().versionIdRef());
        verify(client, never()).listObjects(any(), any(), anyBoolean(), any());
    }

    @Test
    void noMatchesAndEntirelyEmptySelectionFailClearly() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.listObjects("bucket", "", true, Optional.empty())).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(), List.of(), Optional.empty()));
        assertThrows(TrinoException.class, () -> FileSelection.resolve(client, "s3://bucket/*.txt", Map.of()));
        var selection = new FileSelection(List.of(new S3ObjectRef("s3://bucket/empty", 0, null, null)), "");
        assertThrows(TrinoException.class, () -> selection.infer(client, StandardCharsets.UTF_8, (reader, object) -> "unused"));
    }

    @Test
    void escapedMetacharactersResolveToTheExactS3Key() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.getObjectMetadata(anyString()))
                .thenReturn(new S3ClientBuilder.ObjectMetadata(10, Optional.empty(), Optional.empty()));
        String[][] paths = {
                {"file\\*.txt", "file*.txt"},
                {"file\\?.txt", "file?.txt"},
                {"file\\[2026\\].txt", "file[2026].txt"},
                {"file\\{a,b\\}.txt", "file{a,b}.txt"},
                {"folder\\\\name.txt", "folder\\name.txt"}
        };
        for (String[] pair : paths) {
            String expected = "s3://bucket/" + pair[1];
            var selection = FileSelection.resolve(client, "s3://bucket/" + pair[0], Map.of());
            assertEquals(expected, selection.objects().getFirst().path());
            assertEquals("", selection.pathColumn());
            verify(client).getObjectMetadata(expected);
        }
        verify(client, never()).listObjects(any(), any(), anyBoolean(), any());
    }

    @Test
    void escapedPrefixAndActiveWildcardSelectOnlyIntendedObjects() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.maxGlobFiles()).thenReturn(10_000);
        var intended = listed("logs/[2026]/file?.txt", 10);
        var alsoIntended = listed("logs/[2026]/file?-2.txt", 10);
        when(client.listObjects("bucket", "logs/[2026]/file?", true, Optional.empty())).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(intended, alsoIntended,
                        listed("logs/[2026]/file-other.txt", 10), listed("logs/[2026]/file?.csv", 10)),
                        List.of(), Optional.empty()));
        var selection = FileSelection.resolve(client, "s3://bucket/logs/\\[2026\\]/file\\?*.txt", Map.of());
        assertEquals(List.of(alsoIntended.path(), intended.path()), selection.objects().stream().map(S3ObjectRef::path).toList());
        assertEquals("$path", selection.pathColumn());
        verify(client, never()).getObjectMetadata(anyString());
    }

    @Test
    void disabledGlobPreservesRawKeysIncludingInvalidGlobSyntax() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.getObjectMetadata(anyString()))
                .thenReturn(new S3ClientBuilder.ObjectMetadata(10, Optional.of("etag"), Optional.empty()));
        for (String key : List.of("file[2026]*?.txt", "unclosed[", "folder\\name.txt", "trailing\\")) {
            String path = "s3://bucket/" + key;
            var selection = FileSelection.resolve(client, path, Map.of(
                    "GLOB", new ScalarArgument(io.trino.spi.type.BooleanType.BOOLEAN, false)));
            assertEquals(List.of(new S3ObjectRef(path, 10, "etag", null)), selection.objects());
            assertEquals("", selection.pathColumn());
            verify(client).getObjectMetadata(path);
        }
        verify(client, never()).listObjects(any(), any(), anyBoolean(), any());
    }

    @Test
    void selectionPreservesEmptyKeySegments() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.maxGlobFiles()).thenReturn(10_000);
        var doubleSlash = listed("events//part/a.txt", 1);
        when(client.listObjects("bucket", "events/", true, Optional.empty())).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(doubleSlash, listed("events/part/a.txt", 1)),
                        List.of(), Optional.empty()));
        var selection = FileSelection.resolve(client, "s3://bucket/events/*/part/*.txt", Map.of());
        assertEquals(List.of(doubleSlash.path()), selection.objects().stream().map(S3ObjectRef::path).toList());

        when(client.listObjects("bucket", "events//part/", true, Optional.empty())).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(doubleSlash), List.of(), Optional.empty()));
        assertEquals(selection.objects(), FileSelection.resolve(client, "s3://bucket/events//part/*.txt", Map.of()).objects());
    }

    @Test
    void configuredFileLimitCountsUniqueMatchesAcrossPages() {
        var client = mock(S3ClientBuilder.SessionClient.class);
        when(client.maxGlobFiles()).thenReturn(2);
        var a = listed("a.txt", 1);
        var b = listed("b.txt", 1);
        when(client.listObjects("bucket", "", true, Optional.empty())).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(a, listed("ignored.csv", 1)), List.of(), Optional.of("next")));
        when(client.listObjects("bucket", "", true, Optional.of("next"))).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(a, b), List.of(), Optional.empty()));
        assertEquals(2, FileSelection.resolve(client, "s3://bucket/*.txt", Map.of()).objects().size());

        when(client.listObjects("bucket", "", true, Optional.of("next"))).thenReturn(
                new S3ClientBuilder.ListObjectsPage(List.of(a, b, listed("c.txt", 1)), List.of(), Optional.empty()));
        var error = assertThrows(TrinoException.class,
                () -> FileSelection.resolve(client, "s3://bucket/*.txt", Map.of()));
        assertTrue(error.getMessage().contains("more than 2 objects"));
    }

    private static S3ClientBuilder.ListedObject listed(String key, long size) {
        return new S3ClientBuilder.ListedObject("bucket", key, size, Optional.empty(), Optional.of("etag-" + key));
    }
}
