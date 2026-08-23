#!/usr/bin/env bash
# Filter patina noise from a run's output.
grep -v "host-identity\|std_detect\|^patina:  \|deny-trap\|SUD-managed\|^patina: WARNING\|^patina: these\|^PATINA_SDK_REPORT\|^note: checkpoint"
