package marcbp.trino.s3file.list;

import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.util.Optional;

@FunctionalInterface
public interface ListingFetcher {
    ListingPage fetch(S3ClientBuilder.SessionClient sessionClient, Optional<String> continuationToken);
}
