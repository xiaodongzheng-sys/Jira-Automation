# Team Helper Prototype

This folder contains the first local-helper prototype for the team edition.

The helper is intended to run on each user's machine and bridge that user's own BPMIS browser session back to the shared internal portal.

## Intended v1 contract

- `GET /health`
- `POST /bpmis/create-jira`

The current implementation is a scaffold only. It defines a stable shape for the first team-edition experiment without changing the existing local-edition runtime.
