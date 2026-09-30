# tests: the test config helper names a system that belongs to another context

    found:  2026-09-29
    status: open
    verify: cd ../dotfiles && uv run python src/context_leak_check.py --repo ../Cash_Flow_Commander | tail -1

The verify line printed `context-leak: 2 hit(s) in Cash_Flow_Commander` on
2026-09-29 and exits 1. It prints nothing and exits 0 once the two lines are
gone.

`tests/test_utils/config_test_utils.py` builds a list of folders and creates
them. One of the folders is named for a system that belongs to another
context. This repo is public, and the name has no meaning here.

## Evidence

The leak check reports lines 17 and 32 of
`tests/test_utils/config_test_utils.py`: the assignment of one `*_dir`
variable and its place in the `directories` list. The name is not repeated
here.

`grep -rn` for the variable over every `*.py` in the repo finds those two lines
and no reader. The folder it creates under `data/` is empty. The file was last
changed on 2026-07-16.

## fix

1. Delete lines 17 and 32 of `tests/test_utils/config_test_utils.py`.
2. Remove the empty folder it created under `data/`, by its exact path, after
   listing it to confirm it is empty.
3. Run the verify line, then `uv run pytest -q`.

## blast radius

None. Nothing reads the variable. `data/` is gitignored, so removing the empty
folder changes nothing tracked.

## not doing yet

It was found while checking unrelated backlog entries. The name stays in git
history after the fix; whether that matters for a public repo is the user's
call, and rewriting history is a separate decision.
