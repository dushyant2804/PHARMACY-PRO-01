from __future__ import print_function

import sys
import urllib.error
import urllib.request


def fetch(url):
    try:
        response = urllib.request.urlopen(url, timeout=2)
        body = response.read().decode("utf-8", "replace")
        return str(response.getcode()), "", body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        return str(exc.code), str(exc), body
    except Exception as exc:
        return "ERROR", exc.__class__.__name__ + ": " + str(exc), ""


def main():
    if len(sys.argv) != 4:
        sys.stderr.write("Usage: PharmacyOS-Health-Check.py HEALTH_URL DOCS_URL RESULT_FILE\n")
        return 1

    health_url, docs_url, result_file = sys.argv[1], sys.argv[2], sys.argv[3]
    status, error, body = fetch(health_url)
    exit_code = 0 if status == "200" else 1

    docs_status = "NOT_CHECKED"
    docs_error = ""
    docs_body = ""
    if exit_code != 0:
        docs_status, docs_error, docs_body = fetch(docs_url)

    with open(result_file, "w") as output:
        output.write("HTTP status: " + status + "\n")
        output.write("Error: " + (error or "none") + "\n")
        output.write("Response body: " + (body if body else "[empty]") + "\n")
        if exit_code != 0:
            output.write("Docs fallback URL: " + docs_url + "\n")
            output.write("Docs HTTP status: " + docs_status + "\n")
            output.write("Docs error: " + (docs_error or "none") + "\n")
            output.write("Docs response body: " + (docs_body if docs_body else "[empty]") + "\n")
            if docs_status == "200":
                output.write(
                    "Mismatch: health check failed but /docs is reachable. "
                    "Backend is serving Swagger while health endpoint is not returning HTTP 200.\n"
                )
        output.write("Attempt decision: " + ("success" if exit_code == 0 else "failure") + "\n")
        output.write("Attempt exit code: " + str(exit_code) + "\n")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
