#!/bin/sh
# Rebuild the Lean proofs, check that no theorem depends on sorry or a
# non-standard axiom, and regenerate the Go test vectors.
# Requires elan (https://github.com/leanprover/elan); the toolchain is
# pinned in verify/lean-toolchain. See docs/verification.md.
set -eu
cd "$(dirname "$0")"
LAKE="${LAKE:-lake}"
command -v "$LAKE" >/dev/null 2>&1 || LAKE="$HOME/.elan/bin/lake"

"$LAKE" build

out=$("$LAKE" env lean Axioms.lean)
bad=$(printf '%s\n' "$out" | grep -v -e 'does not depend on any axioms' \
	-e "depends on axioms: \[\(propext\)\?\(, \)\?\(Classical.choice\)\?\(, \)\?\(Quot.sound\)\?\]" || true)
if [ -n "$bad" ]; then
	printf 'unexpected axioms:\n%s\n' "$bad" >&2
	exit 1
fi
printf '%s theorems checked, standard axioms only\n' "$(printf '%s\n' "$out" | wc -l | tr -d ' ')"

./.lake/build/bin/gen-vectors ../testdata/verified
