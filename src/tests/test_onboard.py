"""Tests for the external-submissions onboard CLI (src/external-submissions/onboard/main.py)."""

import importlib.util
import os
import sys
from unittest.mock import MagicMock

import pytest

from helpers.constants import RunMode

# "external-submissions" is not an importable package name, so load the modules by path.
_ONBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "external-submissions", "onboard")
sys.path.insert(0, _ONBOARD_DIR)

_spec = importlib.util.spec_from_file_location("main", os.path.join(_ONBOARD_DIR, "main.py"))
onboard = importlib.util.module_from_spec(_spec)
sys.modules["main"] = onboard
_spec.loader.exec_module(onboard)

_ic_spec = importlib.util.spec_from_file_location(
    "init_counters", os.path.join(_ONBOARD_DIR, "init_counters.py")
)
init_counters_module = importlib.util.module_from_spec(_ic_spec)
_ic_spec.loader.exec_module(init_counters_module)

EMAIL = "a@dummy-domain-x92ah8.com"


@pytest.fixture
def fake_google_cloud(monkeypatch):
    """Stub the google.cloud import so register/deactivate can run without GCP libraries."""
    fake = MagicMock()
    fake.firestore.SERVER_TIMESTAMP = "SERVER_TIMESTAMP"
    monkeypatch.setitem(sys.modules, "google", MagicMock())
    monkeypatch.setitem(sys.modules, "google.cloud", fake)
    return fake


@pytest.fixture
def submissions_bucket(monkeypatch):
    """Point env.SUBMISSIONS_BUCKET at a test bucket."""
    monkeypatch.setattr(onboard.env, "SUBMISSIONS_BUCKET", "test-bucket")
    return "test-bucket"


def fake_db():
    """Build a Firestore client stub whose collection()/document() differentiate by name.

    A bare MagicMock returns the same child for any argument, so a query against the wrong
    collection or document ID would still pass. This stub returns a distinct mock per name;
    document snapshots default to exists=False.
    """
    db = MagicMock()
    collections = {}

    def get_collection(name):
        if name not in collections:
            collection = MagicMock()
            documents = {}

            def get_document(doc_id, _documents=documents):
                if doc_id not in _documents:
                    document = MagicMock()
                    document.get.return_value.exists = False
                    _documents[doc_id] = document
                return _documents[doc_id]

            collection.document.side_effect = get_document
            collections[name] = collection
        return collections[name]

    db.collection.side_effect = get_collection
    return db


class TestNormalizeName:
    """Test case-insensitive name normalization, including anonymous forms."""

    def test_lowercases_and_collapses_whitespace(self):
        assert onboard.normalize_name("  GDM   A ") == "gdm a"

    def test_legacy_anonymous_csv_form_matches_canonical(self):
        assert onboard.normalize_name("Anonymous #8") == onboard.normalize_name("anonymous 8")

    def test_hash_only_normalized_for_anonymous_names(self):
        assert onboard.normalize_name("Team #1") == "team #1"


class TestSlugifyOrganization:
    """Test filename-safe slugs for team folder names."""

    def test_spaces_and_case(self):
        assert onboard.slugify_organization("Example Research Lab") == "example-research-lab"

    def test_punctuation_collapses_to_hyphens(self):
        assert onboard.slugify_organization(" cmcc.vc ") == "cmcc-vc"

    def test_accents_transliterate(self):
        assert onboard.slugify_organization("Gréta Labs") == "greta-labs"

    def test_truncates_long_names_without_trailing_hyphen(self):
        slug = onboard.slugify_organization(
            "An Extremely Long Organization Name That Exceeds The Slug Limit For Sure"
        )
        assert len(slug) <= onboard.MAX_SLUG_LENGTH
        assert not slug.endswith("-")

    def test_nothing_left_falls_back_to_team(self):
        assert onboard.slugify_organization("???") == "team"
        assert onboard.slugify_organization("森林实验室") == "team"


class TestGenerateTeamId:
    """Test slug + 6-char-hash team ID generation."""

    def test_format(self, monkeypatch):
        monkeypatch.setattr(onboard.secrets, "token_hex", lambda n: "ab12cd")
        assert onboard.generate_team_id(fake_db(), "Acme Corp") == "acme-corp_ab12cd"

    def test_retries_on_collision(self, monkeypatch):
        hashes = iter(["aaaaaa", "bbbbbb"])
        monkeypatch.setattr(onboard.secrets, "token_hex", lambda n: next(hashes))
        db = fake_db()
        db.collection("teams").document("acme_aaaaaa").get.return_value.exists = True
        assert onboard.generate_team_id(db, "Acme") == "acme_bbbbbb"

    def test_anonymous_display_name_slugs(self, monkeypatch):
        monkeypatch.setattr(onboard.secrets, "token_hex", lambda n: "ab12cd")
        assert onboard.generate_team_id(fake_db(), "Anonymous 9") == "anonymous-9_ab12cd"


class TestGetClients:
    """Test client construction guards (no GCP call is made before the guard)."""

    def test_refuses_without_cloud_project(self, monkeypatch):
        monkeypatch.setattr(onboard.env, "PROJECT_ID", None)
        with pytest.raises(RuntimeError, match="CLOUD_PROJECT is not set"):
            onboard.get_clients()


class TestMakePrincipal:
    """Test IAM principal formatting."""

    def test_user_email(self):
        assert onboard.make_principal(EMAIL) == f"user:{EMAIL}"

    def test_service_account(self):
        sa = "uploader@proj.iam.gserviceaccount.com"
        assert onboard.make_principal(sa) == f"serviceAccount:{sa}"


class TestIsGoogleAccount:
    """Test Google account detection (fast path only; MX lookups are not exercised)."""

    def test_gmail_fast_path(self):
        assert onboard.is_google_account("someone@gmail.com")

    def test_unresolvable_domain_is_not_google(self):
        assert not onboard.is_google_account("someone@invalid.invalid")


class TestBuildWelcomeEmail:
    """Test welcome email content requirements."""

    def test_contains_folder_due_date_and_wiki_link_only(self, submissions_bucket):
        _, body = onboard.build_welcome_email("acme_ab12cd", "Acme Corp", False, "2026-06-21")
        assert "gs://test-bucket/acme_ab12cd/" in body
        assert "2026-06-21" in body
        assert onboard.SUBMISSION_WIKI_URL in body
        # Submission steps live on the wiki only; the email must not duplicate them.
        assert "gsutil" not in body
        assert "gcloud" not in body

    def test_anonymous_note_wording_and_presence(self, submissions_bucket):
        _, body = onboard.build_welcome_email(
            "anonymous-9_ab12cd", "Anonymous 9", True, "2026-06-21"
        )
        assert "use it as 'organization'" in body
        assert "choose whether to also use it for" in body
        _, body = onboard.build_welcome_email("acme_ab12cd", "Acme Corp", False, "2026-06-21")
        assert "registered anonymously" not in body


class TestFolderPermissions:
    """Test per-folder IAM binding management."""

    @staticmethod
    def _gcs_with_policy(bindings):
        policy = MagicMock()
        policy.bindings = bindings
        gcs = MagicMock()
        gcs.bucket.return_value.get_iam_policy.return_value = policy
        return gcs, policy

    def test_set_is_idempotent_and_keeps_unrelated_bindings(self):
        prefix = onboard.folder_prefix("test-bucket", "acme_ab12cd")
        stale = {
            "role": "roles/storage.objectUser",
            "members": {"user:old@dummy-domain-x92ah8.com"},
            "condition": {"expression": f'resource.name.startsWith("{prefix}")'},
        }
        other = {
            "role": "roles/storage.objectUser",
            "members": {"user:other@dummy-domain-x92ah8.com"},
            "condition": {
                "expression": (
                    'resource.name.startsWith("projects/_/buckets/test-bucket/objects/zeta_9f8e7d/")'
                )
            },
        }
        gcs, policy = self._gcs_with_policy([stale, other])
        onboard.set_folder_permissions(gcs, "test-bucket", "acme_ab12cd", {f"user:{EMAIL}"})
        assert other in policy.bindings
        assert stale not in policy.bindings
        team_bindings = [b for b in policy.bindings if prefix in b["condition"]["expression"]]
        assert {b["role"] for b in team_bindings} == {
            "roles/storage.objectViewer",
            "roles/storage.objectUser",
        }
        assert all(b["members"] == {f"user:{EMAIL}"} for b in team_bindings)

    def test_remove_drops_only_team_bindings(self):
        prefix = onboard.folder_prefix("test-bucket", "acme_ab12cd")
        team = {
            "role": "roles/storage.objectUser",
            "members": {f"user:{EMAIL}"},
            "condition": {"expression": f'resource.name.startsWith("{prefix}")'},
        }
        unconditional = {
            "role": "roles/storage.admin",
            "members": {"user:admin@dummy-domain-x92ah8.com"},
        }
        gcs, policy = self._gcs_with_policy([team, unconditional])
        onboard.remove_folder_permissions(gcs, "test-bucket", "acme_ab12cd")
        assert policy.bindings == [unconditional]


class TestRegister:
    """Test team registration, including the run-mode email matrix."""

    @pytest.fixture
    def wiring(self, monkeypatch, submissions_bucket, fake_google_cloud):
        """Patch GCP-touching internals; capture email sends and IAM grants."""
        state = {"sent": None, "principals": None, "id_source": None}
        monkeypatch.setattr(onboard, "allocate_anon_number", lambda db: 9)

        def _generate(db, organization):
            state["id_source"] = organization
            return f"{onboard.slugify_organization(organization)}_ab12cd"

        monkeypatch.setattr(onboard, "generate_team_id", _generate)
        monkeypatch.setattr(
            onboard,
            "set_folder_permissions",
            lambda gcs, bucket, team_id, principals: state.update(principals=principals),
        )
        monkeypatch.setattr(onboard, "is_google_account", lambda e: True)

        def _send(to_emails, subject, body, run_mode, send_email_in_test):
            state.update(sent={"to": to_emails, "mode": run_mode, "in_test": send_email_in_test})
            return True

        monkeypatch.setattr(onboard.email, "send_email", _send)
        return state

    def test_requires_organization(self, submissions_bucket):
        with pytest.raises(ValueError, match="organization"):
            onboard.register(organization="", emails=[EMAIL], db=MagicMock(), gcs=MagicMock())

    def test_requires_emails_or_service_accounts(self, submissions_bucket):
        with pytest.raises(ValueError, match="at least one"):
            onboard.register(organization="Acme", db=MagicMock(), gcs=MagicMock())

    def test_rejects_invalid_email(self, submissions_bucket):
        with pytest.raises(ValueError, match="Invalid email"):
            onboard.register(
                organization="Acme", emails=["not-an-email"], db=MagicMock(), gcs=MagicMock()
            )

    def test_requires_submissions_bucket(self, monkeypatch):
        monkeypatch.setattr(onboard.env, "SUBMISSIONS_BUCKET", None)
        with pytest.raises(ValueError, match="SUBMISSIONS_BUCKET"):
            onboard.register(organization="Acme", emails=[EMAIL], db=MagicMock(), gcs=MagicMock())

    def test_rejects_reserved_team_name(self, submissions_bucket):
        db = fake_db()
        # The reservation must be looked up in team_names under the normalized name.
        db.collection("team_names").document("gdm a").get.return_value.exists = True
        with pytest.raises(ValueError, match="reserved"):
            onboard.register(
                organization="Acme",
                emails=[EMAIL],
                team_name="GDM A",
                db=db,
                gcs=MagicMock(),
            )

    def test_happy_path_anonymous(self, wiring):
        db, gcs = fake_db(), MagicMock()

        result = onboard.register(
            organization="Acme Corp",
            emails=["MiXeD@Dummy-Domain-X92ah8.COM"],
            service_accounts=["uploader@proj.iam.gserviceaccount.com"],
            team_name="acme-a",
            anonymous=True,
            db=db,
            gcs=gcs,
        )

        # The folder slug comes from the public (anonymous) name, never the real org.
        assert wiring["id_source"] == "Anonymous 9"
        assert result["team_id"] == "anonymous-9_ab12cd"
        assert result["organization"] == "Anonymous 9"
        assert result["team_name"] == "acme-a"
        assert wiring["principals"] == {
            "user:mixed@dummy-domain-x92ah8.com",
            "serviceAccount:uploader@proj.iam.gserviceaccount.com",
        }
        gcs.bucket.return_value.blob.assert_called_with("anonymous-9_ab12cd/.keep")
        saved = db.collection("teams").document("anonymous-9_ab12cd").set.call_args[0][0]
        assert saved["organization"] == "Anonymous 9"
        assert saved["deanonymized_organization"] == "Acme Corp"
        assert saved["active"] is True
        reservation = db.collection("team_names").document("acme-a").set.call_args[0][0]
        assert reservation["team_id"] == "anonymous-9_ab12cd"

    def test_default_test_mode_skips_email_with_warning(self, wiring):
        result = onboard.register(
            organization="Acme", emails=[EMAIL], db=fake_db(), gcs=MagicMock()
        )
        assert wiring["sent"] is None
        assert result["welcome_email_sent"] is False
        assert result["run_mode"] == "TEST"
        assert any(
            "skipped in TEST mode" in w and "--send-email-in-test" in w for w in result["warnings"]
        )
        assert any(EMAIL in w for w in result["warnings"])

    def test_test_mode_with_flag_sends_rerouted(self, wiring):
        result = onboard.register(
            organization="Acme",
            emails=[EMAIL],
            send_email_in_test=True,
            db=fake_db(),
            gcs=MagicMock(),
        )
        assert wiring["sent"] == {"to": [EMAIL], "mode": RunMode.TEST, "in_test": True}
        assert result["welcome_email_sent"] is True

    def test_prod_mode_sends_normally(self, wiring):
        result = onboard.register(
            organization="Acme",
            emails=[EMAIL],
            run_mode=RunMode.PROD,
            db=fake_db(),
            gcs=MagicMock(),
        )
        assert wiring["sent"] == {"to": [EMAIL], "mode": RunMode.PROD, "in_test": False}
        assert result["welcome_email_sent"] is True
        assert result["run_mode"] == "PROD"

    def test_service_accounts_only_never_emails(self, wiring):
        result = onboard.register(
            organization="Bot Org",
            service_accounts=["uploader@proj.iam.gserviceaccount.com"],
            run_mode=RunMode.PROD,
            db=fake_db(),
            gcs=MagicMock(),
        )
        assert wiring["sent"] is None
        assert result["welcome_email_sent"] is False

    def test_nonexistent_identity_raises_clean_error(self, wiring, monkeypatch):
        def _reject(gcs, bucket, team_id, principals):
            raise RuntimeError("User a@dummy-domain-x92ah8.com does not exist.")

        monkeypatch.setattr(onboard, "set_folder_permissions", _reject)
        with pytest.raises(ValueError, match="existing Google identities"):
            onboard.register(organization="Ghost", emails=[EMAIL], db=fake_db(), gcs=MagicMock())

    def test_non_google_email_warns_but_registers(self, wiring, monkeypatch):
        monkeypatch.setattr(onboard, "is_google_account", lambda e: False)
        result = onboard.register(
            organization="Acme", emails=[EMAIL], db=fake_db(), gcs=MagicMock()
        )
        assert result["team_id"] == "acme_ab12cd"
        assert any("Google" in w for w in result["warnings"])

    def test_same_org_twice_gets_distinct_ids(self, wiring, monkeypatch):
        hashes = iter(["aaaaaa", "bbbbbb"])
        monkeypatch.setattr(
            onboard,
            "generate_team_id",
            lambda db, org: f"{onboard.slugify_organization(org)}_{next(hashes)}",
        )
        db = fake_db()
        first = onboard.register(organization="Acme", emails=[EMAIL], db=db, gcs=MagicMock())
        second = onboard.register(organization="Acme", emails=[EMAIL], db=db, gcs=MagicMock())
        assert first["team_id"] != second["team_id"]


class TestDeactivate:
    """Test team deactivation."""

    def test_not_found(self, submissions_bucket):
        # fake_db snapshots default to exists=False.
        with pytest.raises(ValueError, match="not found"):
            onboard.deactivate("acme_ab12cd", db=fake_db(), gcs=MagicMock())

    def test_already_inactive(self, submissions_bucket):
        db = fake_db()
        snapshot = db.collection("teams").document("acme_ab12cd").get.return_value
        snapshot.exists = True
        snapshot.to_dict.return_value = {"active": False}
        with pytest.raises(ValueError, match="already inactive"):
            onboard.deactivate("acme_ab12cd", db=db, gcs=MagicMock())

    def test_deactivates_and_revokes(self, monkeypatch, submissions_bucket, fake_google_cloud):
        revoked = {}
        monkeypatch.setattr(
            onboard,
            "remove_folder_permissions",
            lambda gcs, bucket, team_id: revoked.update(team_id=team_id),
        )
        db = fake_db()
        snapshot = db.collection("teams").document("acme_ab12cd").get.return_value
        snapshot.exists = True
        snapshot.to_dict.return_value = {"active": True}

        result = onboard.deactivate("acme_ab12cd", db=db, gcs=MagicMock())

        assert revoked["team_id"] == "acme_ab12cd"
        assert result["active"] is False
        updated = db.collection("teams").document("acme_ab12cd").update.call_args[0][0]
        assert updated["active"] is False


class TestInitCounters:
    """Test the one-time counter setup script."""

    def test_refuses_to_overwrite(self):
        db = fake_db()
        db.collection("counters").document("teams").get.return_value.exists = True
        with pytest.raises(ValueError, match="already exists"):
            init_counters_module.init_counters(anon_count=8, db=db)

    def test_creates_counter(self):
        db = fake_db()
        result = init_counters_module.init_counters(anon_count=8, db=db)
        assert result == {"anon_count": 8}
        db.collection("counters").document("teams").set.assert_called_once_with({"anon_count": 8})
