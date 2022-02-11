from requests.exceptions import HTTPError


class DagsterCloudHTTPError(Exception):
    """
    Clearer error message for exceptions hitting Dagster Cloud servers.
    """

    def __init__(self, http_error: HTTPError):
        self.response = http_error.response
        error_content = http_error.response.content.decode("utf-8", errors="ignore")
        super(DagsterCloudHTTPError, self).__init__(
            http_error.__str__() + ": " + str(error_content)
        )


def raise_http_error(response):
    try:
        response.raise_for_status()
    except HTTPError as e:
        raise DagsterCloudHTTPError(e) from e
