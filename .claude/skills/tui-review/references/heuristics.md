# Terminal UX review prompts

Use the sections relevant to the requested review. These are investigation
prompts, not universal pass/fail rules or a required report outline.

## Input and navigation

Check whether focus and selection are apparent and shortcuts are discoverable.
Exercise the application's documented navigation, help, and exit paths. In text
entry, try punctuation, paths such as `foo/bar`, pasted multiline text, and Unicode
when supported. Distinguish intentional command triggers from accidental input
capture. Check whether dismissing an overlay preserves unfinished input and
returns focus predictably.

## Feedback and responsiveness

Observe the transitions that matter: ready, working, streaming, completed,
cancelled, and failed. Can the user tell whether an action was accepted and
whether it is safe to retry? Does slow work leave navigation and cancellation
usable? Check for lost or queued input and stale progress displays.

Polling a tmux pane does not establish precise application latency. If latency
matters, measure it with appropriate instrumentation and state the method.
Some keys correctly do nothing at a boundary; an unchanged frame is not itself
evidence of a defect.

## Errors, empty states, and recovery

Try a relevant invalid input or unavailable resource using disposable data.
Check that the error explains the failure and offers a useful next action without
losing user work. Distinguish an empty result from loading or failure where that
difference matters. Avoid inventing errors or requiring text in every blank area.

## Layout and terminal compatibility

Exercise realistic terminal sizes and a smaller supported size. Check for
overlapping content, clipped actions, lost selection, broken scrolling, and
redraw artifacts during resize. Include long values and wide/combining characters
when relevant. An explicitly unsupported size may display a useful minimum-size
message instead of fitting every control.

Inspect focus, hierarchy, contrast, and status distinctions in the actual theme.
Do not rely on color alone for essential meaning. A monochrome interface or a
simple selection marker can be effective. Evaluate borders by their contribution
to grouping and readability, rather than their presence or count.

## Consequential actions

For destructive or externally visible actions, evaluate the app's existing
authorization model: is the target and consequence clear, and can the user cancel
or recover where appropriate? Test with disposable targets. An already-authorized
batch workflow need not ask for confirmation at every step.
