"""Tests for the `validate` connection/config probe (Phase 2).

cmd_validate probes every configured integration and prints a ✓/✗ line per
service. Required services (Trakt, Sonarr, Jellyfin) failing => returns False
(exit 1); only-optional services unset => returns True (exit 0) with an info
note. Every probe must catch its own exceptions. No real network.

Probe seams:
  - Trakt/TMDB/Seerr go through the named helpers (trakt_get/tmdb_get/seerr_get),
    which already return parsed JSON — so those are patched directly.
  - Sonarr/Jellyfin/SABnzbd build requests locally via _api_request_with_retry,
    which also returns parsed JSON — patched to drive each branch.
"""

import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import trakt_discovery


VALID_UUID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"


def _run_validate(send=False):
    """Run cmd_validate with a dummy conn, capturing stdout. Returns (ok, output)."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        ok = trakt_discovery.cmd_validate(conn=object(), send=send)
    return ok, buf.getvalue()


def _all_good_env(**overrides):
    """Context managers that configure every integration with sane defaults.

    Returns a list of patch objects; callers add per-test behavior patches.
    """
    cfg = {
        "TMDB_API_KEY": "tmdb-secret-key",
        "SEERR_URL": "http://seerr:5055",
        "SEERR_API_KEY": "seerr-secret",
        "SONARR_URL": "http://sonarr:8989",
        "SONARR_API_KEY": "sonarr-secret",
        "RADARR_URL": "http://radarr:7878",
        "RADARR_API_KEY": "radarr-secret",
        "JELLYFIN_URL": "http://jellyfin:8096",
        "JELLYFIN_API_KEY": "jelly-secret",
        "JELLYFIN_USER_IDS": [VALID_UUID],
        "SABNZBD_URL": "http://sab:8080",
        "SABNZBD_API_KEY": "sab-secret",
        "ALERT_WEBHOOK_URL": "http://hook/token-secret",
        "SMTP_HOST": "",
        "ALERT_EMAIL_TO": "",
        "ALERT_EMAIL_FROM": "",
    }
    cfg.update(overrides)
    return [patch.object(trakt_discovery, k, v) for k, v in cfg.items()]


# A future expiry so the Trakt token reads as valid.
def _future_tokens(days=5):
    from datetime import datetime, timedelta, timezone
    exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    return {"access_token": "a", "refresh_token": "r", "expires_at": exp}


class _ProbeHarness(unittest.TestCase):
    """Base: patch all named-helper probes good by default; subclasses override."""

    def setUp(self):
        self._stack = []

    def _enter(self, *patchers):
        for p in patchers:
            p.start()
            self._stack.append(p)

    def tearDown(self):
        for p in reversed(self._stack):
            p.stop()

    def _good_helpers(self):
        """Patch every probe helper so all services pass. Returns the patch list
        (already started via _enter in the caller)."""
        return [
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value={"all": 1}),
            patch.object(trakt_discovery, "tmdb_get", return_value={"images": {}}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "1.2.3"}),
            patch.object(
                trakt_discovery,
                "_api_request_with_retry",
                side_effect=self._fake_local_api,
            ),
        ]

    @staticmethod
    def _fake_local_api(method, url, headers, **kwargs):
        """Stand-in for _api_request_with_retry covering Sonarr/Jellyfin/SAB."""
        if "/api/v3/system/status" in url:
            return {"version": "4.0.1"}
        if url.endswith("/System/Info"):
            return {"Version": "10.9.0"}
        if "/Users/" in url:
            return {"Name": "Shane"}
        if url.endswith("/api"):  # SABnzbd
            return {"version": "4.1.0"}
        raise AssertionError(f"unexpected URL {url}")


class AllGoodTests(_ProbeHarness):
    def test_every_service_ok_and_exit_zero(self):
        self._enter(*_all_good_env())
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertTrue(ok)
        for svc in ("Trakt", "TMDB", "Seerr", "Sonarr", "Radarr", "Jellyfin", "SABnzbd", "Alerts"):
            self.assertIn(f"✓ {svc}", out, f"{svc} missing/failed:\n{out}")
        self.assertNotIn("✗", out)
        self.assertIn("Validation OK", out)


class TraktTests(_ProbeHarness):
    def _base(self):
        self._enter(*_all_good_env())

    def test_trakt_missing_tokens_is_required_fail(self):
        self._base()
        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=None),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=self._fake_local_api),
        )
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Trakt", out)

    def test_trakt_expired_token_fails(self):
        self._base()
        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens(days=-1)),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=self._fake_local_api),
        )
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("EXPIRED", out)

    def test_trakt_ping_failure_fails(self):
        self._base()
        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value=None),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=self._fake_local_api),
        )
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Trakt", out)

    def test_trakt_exception_is_caught_and_other_probes_run(self):
        self._base()
        self._enter(
            patch.object(trakt_discovery, "load_tokens", side_effect=RuntimeError("boom")),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=self._fake_local_api),
        )
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Trakt", out)
        # other probes still ran
        self.assertIn("✓ Sonarr", out)
        self.assertIn("✓ Jellyfin", out)


class SonarrTests(_ProbeHarness):
    def test_sonarr_failure_is_required_fail(self):
        self._enter(*_all_good_env())

        def fake(method, url, headers, **kw):
            if "/api/v3/system/status" in url:
                return None
            return _ProbeHarness._fake_local_api(method, url, headers, **kw)

        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value={}),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=fake),
        )
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Sonarr", out)

    def test_sonarr_unset_is_required_fail(self):
        self._enter(*_all_good_env(SONARR_URL="", SONARR_API_KEY=""))
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Sonarr", out)


class RadarrTests(_ProbeHarness):
    def test_radarr_ok_renders_check(self):
        self._enter(*_all_good_env())
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertTrue(ok)
        self.assertIn("✓ Radarr", out)

    def test_radarr_failure_is_not_required_fail(self):
        self._enter(*_all_good_env())

        # Radarr and Sonarr share the /api/v3/system/status path; distinguish by
        # host so only Radarr fails here.
        def fake(method, url, headers, **kw):
            if "/api/v3/system/status" in url and "radarr" in url:
                return None
            return _ProbeHarness._fake_local_api(method, url, headers, **kw)

        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value={}),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=fake),
        )
        ok, out = _run_validate()
        self.assertTrue(ok)  # Radarr is optional
        self.assertIn("✗ Radarr", out)
        self.assertIn("✓ Sonarr", out)

    def test_radarr_unset_is_skipped_not_required_fail(self):
        self._enter(*_all_good_env(RADARR_URL="", RADARR_API_KEY=""))
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertTrue(ok)
        self.assertNotIn("✓ Radarr", out)
        self.assertNotIn("✗ Radarr", out)
        self.assertIn("optional integrations not configured", out)
        self.assertIn("Radarr", out)


class JellyfinTests(_ProbeHarness):
    def test_uuid_user_resolves(self):
        self._enter(*_all_good_env(JELLYFIN_USER_IDS=[VALID_UUID]))
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertTrue(ok)
        self.assertIn(f"✓ Jellyfin user '{VALID_UUID}'", out)

    def test_display_name_flagged(self):
        self._enter(*_all_good_env(JELLYFIN_USER_IDS=["azure-aperture"]))
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Jellyfin user 'azure-aperture'", out)
        self.assertIn("UUID", out)

    def test_server_unreachable_fails(self):
        self._enter(*_all_good_env())

        def fake(method, url, headers, **kw):
            if url.endswith("/System/Info"):
                return None
            return _ProbeHarness._fake_local_api(method, url, headers, **kw)

        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value={}),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=fake),
        )
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Jellyfin", out)

    def test_empty_user_ids_fails(self):
        self._enter(*_all_good_env(JELLYFIN_USER_IDS=[]))
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertFalse(ok)
        self.assertIn("✗ Jellyfin users", out)


class OptionalServiceTests(_ProbeHarness):
    def test_only_optional_unset_exits_zero_with_note(self):
        # All optional services unset; required ones good.
        self._enter(*_all_good_env(
            TMDB_API_KEY="",
            SEERR_URL="", SEERR_API_KEY="",
            SABNZBD_URL="", SABNZBD_API_KEY="",
            ALERT_WEBHOOK_URL="", SMTP_HOST="", ALERT_EMAIL_TO="", ALERT_EMAIL_FROM="",
        ))
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertTrue(ok)
        self.assertIn("optional integrations not configured", out)
        for svc in ("TMDB", "Seerr", "SABnzbd", "Alerts"):
            self.assertIn(svc, out)

    def test_tmdb_skipped_when_no_key(self):
        self._enter(*_all_good_env(TMDB_API_KEY=""))
        self._enter(*self._good_helpers())
        ok, out = _run_validate()
        self.assertTrue(ok)
        self.assertNotIn("✓ TMDB", out)
        self.assertNotIn("✗ TMDB", out)

    def test_seerr_failure_does_not_break_required(self):
        self._enter(*_all_good_env())
        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value={}),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value=None),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=self._fake_local_api),
        )
        ok, out = _run_validate()
        self.assertTrue(ok)  # Seerr is optional
        self.assertIn("✗ Seerr", out)

    def test_sabnzbd_failure_does_not_break_required(self):
        self._enter(*_all_good_env())

        def fake(method, url, headers, **kw):
            if url.endswith("/api"):
                return None
            return _ProbeHarness._fake_local_api(method, url, headers, **kw)

        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value={}),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=fake),
        )
        ok, out = _run_validate()
        self.assertTrue(ok)
        self.assertIn("✗ SABnzbd", out)


class AlertsTests(_ProbeHarness):
    def test_alerts_report_only_lists_channels(self):
        self._enter(*_all_good_env(ALERT_WEBHOOK_URL="http://hook/secret"))
        self._enter(*self._good_helpers())
        ok, out = _run_validate(send=False)
        self.assertTrue(ok)
        self.assertIn("✓ Alerts", out)
        self.assertIn("--send", out)

    def test_alerts_send_fires_test(self):
        self._enter(*_all_good_env(ALERT_WEBHOOK_URL="http://hook/secret"))
        self._enter(*self._good_helpers())
        self._enter(patch.object(trakt_discovery, "cmd_test_alert", return_value=True))
        ok, out = _run_validate(send=True)
        self.assertTrue(ok)
        self.assertIn("test message sent", out)


class RedactionTests(_ProbeHarness):
    def test_no_secret_substrings_in_output(self):
        # Drive an error path that echoes a URL containing a key query param.
        self._enter(*_all_good_env(
            SABNZBD_URL="http://sab:8080",
            SABNZBD_API_KEY="SUPERSECRETSAB",
            TMDB_API_KEY="TMDBSECRET123",
        ))

        def fake(method, url, headers, **kw):
            # Echo a SAB-style URL with the apikey in an exception string.
            if url.endswith("/api"):
                raise RuntimeError(f"failed: {url}?apikey=SUPERSECRETSAB&mode=version")
            return _ProbeHarness._fake_local_api(method, url, headers, **kw)

        self._enter(
            patch.object(trakt_discovery, "load_tokens", return_value=_future_tokens()),
            patch.object(trakt_discovery, "trakt_get", return_value={}),
            patch.object(trakt_discovery, "tmdb_get", return_value={}),
            patch.object(trakt_discovery, "seerr_get", return_value={"version": "x"}),
            patch.object(trakt_discovery, "_api_request_with_retry", side_effect=fake),
        )
        ok, out = _run_validate()
        self.assertNotIn("SUPERSECRETSAB", out)
        self.assertIn("REDACTED", out)


if __name__ == "__main__":
    unittest.main()
