package marcbp.trino.s3file.list;

import java.util.List;
import java.util.Optional;

public record ListingPage(List<ListingRow> rows, Optional<String> nextContinuationToken) {}
