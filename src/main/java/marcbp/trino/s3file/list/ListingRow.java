package marcbp.trino.s3file.list;

public interface ListingRow {
    Object valueFor(String columnName);
}
