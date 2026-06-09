"""LeadGurus entry point."""

from __future__ import annotations

from tap_leadgurus.tap import TapLeadGurus

if __name__ == "__main__":
    TapLeadGurus.cli()
