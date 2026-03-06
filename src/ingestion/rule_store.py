"""
Rule Store — Stage 4 of the Write Path.

Persists extracted rules as structured JSON for fast querying.
The rules are derived from the PDF source of truth and stored as
queryable JSON for the deterministic read path.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from ..models.rules import TariffRule

logger = logging.getLogger(__name__)

# Default storage directory
DEFAULT_STORE_DIR = Path(__file__).parent.parent.parent / "data" / "extracted_rules"


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


class RuleStore:
    """
    Persistent store for extracted tariff rules.

    Rules are derived data that can be re-extracted from the
    source PDF at any time.
    """

    def __init__(self, store_dir: str | Path | None = None):
        self.store_dir = Path(store_dir) if store_dir else DEFAULT_STORE_DIR
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self._rules: dict[str, list[TariffRule]] = {}  # keyed by document_id

    def save_rules(
        self,
        rules: list[TariffRule],
        document_name: str,
        source_hash: str = "",
    ) -> Path:
        """
        Persist rules to JSON file.

        The file is named by document and includes metadata for
        traceability (extraction timestamp, model used, source hash).
        """
        doc_id = self._sanitize_name(document_name)
        self._rules[doc_id] = rules

        output = {
            "metadata": {
                "document": document_name,
                "source_hash": source_hash,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "rule_count": len(rules),
                "model": "gemini-2.0-flash",
            },
            "rules": [rule.model_dump(mode="json") for rule in rules],
        }

        filepath = self.store_dir / f"{doc_id}.json"
        filepath.write_text(
            json.dumps(output, indent=2, cls=DecimalEncoder),
            encoding="utf-8",
        )

        logger.info("Saved %d rules to %s", len(rules), filepath)
        return filepath

    def load_rules(self, document_name: str | None = None) -> list[TariffRule]:
        """
        Load rules from stored JSON.

        If document_name is None, loads all rules from all files.
        """
        if document_name:
            doc_id = self._sanitize_name(document_name)
            filepath = self.store_dir / f"{doc_id}.json"
            if filepath.exists():
                return self._load_from_file(filepath)
            return []

        # Load all rule files
        all_rules = []
        for filepath in sorted(self.store_dir.glob("*.json")):
            all_rules.extend(self._load_from_file(filepath))
        return all_rules

    def get_rules_by_port(self, port: str) -> list[TariffRule]:
        """Get all rules for a specific port."""
        all_rules = self.load_rules()
        return [r for r in all_rules if r.port.lower() == port.lower()]

    def get_rules_by_due_type(self, port: str, due_type: str) -> list[TariffRule]:
        """Get rules for a specific port and due type."""
        port_rules = self.get_rules_by_port(port)
        return [r for r in port_rules if r.due_type == due_type]

    def get_available_ports(self) -> list[str]:
        """List all ports that have extracted rules."""
        all_rules = self.load_rules()
        return sorted(set(r.port for r in all_rules))

    def get_available_due_types(self) -> list[str]:
        """List all due types that have extracted rules."""
        all_rules = self.load_rules()
        return sorted(set(r.due_type for r in all_rules))

    def _load_from_file(self, filepath: Path) -> list[TariffRule]:
        """Load rules from a single JSON file."""
        try:
            data = json.loads(filepath.read_text(encoding="utf-8"))
            rules = [TariffRule.model_validate(r) for r in data.get("rules", [])]
            logger.info("Loaded %d rules from %s", len(rules), filepath.name)
            return rules
        except Exception as e:
            logger.error("Failed to load rules from %s: %s", filepath, e)
            return []

    @staticmethod
    def compute_file_hash(filepath: str | Path) -> str:
        """Compute SHA-256 hash of a file for change detection."""
        h = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Convert document name to filesystem-safe identifier."""
        return (
            name.lower()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("/", "_")
            .rstrip("_")
        )
