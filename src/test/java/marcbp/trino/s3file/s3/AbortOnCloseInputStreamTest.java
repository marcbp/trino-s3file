package marcbp.trino.s3file.s3;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class AbortOnCloseInputStreamTest {
    @SuppressWarnings("unchecked")
    private static ResponseInputStream<GetObjectResponse> response(Long length) {
        ResponseInputStream<GetObjectResponse> response = mock(ResponseInputStream.class);
        when(response.response()).thenReturn(GetObjectResponse.builder().contentLength(length).build());
        return response;
    }

    @Test
    void abortsPartialResponseOnlyOnce() throws IOException {
        var response = response(100L);
        when(response.read()).thenReturn(65);
        var input = new AbortOnCloseInputStream(response);
        assertEquals(65, input.read());
        input.close();
        input.close();
        verify(response).abort();
        verify(response, never()).close();
    }

    @Test
    void reusesFullyConsumedResponse() throws IOException {
        var response = response(4L);
        byte[] bytes = new byte[2];
        when(response.read()).thenReturn(65);
        when(response.read(bytes, 0, 2)).thenReturn(2);
        when(response.skip(1)).thenReturn(1L);
        var input = new AbortOnCloseInputStream(response);
        assertEquals(65, input.read());
        assertEquals(2, input.read(bytes));
        assertEquals(1, input.skip(1));
        input.close();
        verify(response).close();
        verify(response, never()).abort();
    }

    @Test
    void abortsUnknownLengthUnlessEofWasRead() throws IOException {
        var partial = response(null);
        new AbortOnCloseInputStream(partial).close();
        verify(partial).abort();

        var complete = response(null);
        when(complete.read()).thenReturn(65, -1);
        var input = new AbortOnCloseInputStream(complete);
        assertEquals(65, input.read());
        assertEquals(-1, input.read());
        input.close();
        verify(complete).close();
        verify(complete, never()).abort();
    }

    @Test
    void abortsAfterReadFailure() throws IOException {
        var response = response(100L);
        when(response.read()).thenThrow(new IOException("broken connection"));
        var input = new AbortOnCloseInputStream(response);
        assertThrows(IOException.class, input::read);
        input.close();
        verify(response).abort();
    }
}
