# CRDT Table

This is proof of concept for Conflict-free Replicated Data Type representing 2-dimensional table. Planned:

- [x] Insert or update existing rows (conflict resolution timestamp per cell basis)
- [x] Insert new series of rows in between existing rows.
- [x] Automatically add new columns when necessary.
- [x] Insert new columns in between existing ones.
- [x] Delete entire rows.
- [x] Delete entire columns.
- Selections - conflict-free projections of part of table:
    - [ ] Get part of the data correlated to given selection.
    - [ ] Resize boundaries of selection.
    - [ ] Delete selection.
    - [ ] Update user metadata attached to selection object.
    - [ ] Delete user metadata attached to selection object.
    - [ ] Find all selections intersecting with a given one in scope of a table.
    - [ ] Observe all changes happening within the scope of a selection.

## Conflict resolution details

### Versioning

This implementation uses a combination `{PeerID,HybridLogicalTimestamp}` as a means of conflict resolution, which on positive side is quite lightweight, but otherwise is pretty cumbersome:

- clock drifts on client devices
- no ability to recognize concurrent updates ie. conflict resolution for things like update cell / delete column now depends on the clock timestamp
 
 Easiest proposal is to switch to vector versions, with optional clock timestamps where useful, but they can be quite heavy (especially when on per cell basis). Another proposal is to use partially-ordered log to detect concurrent conflicts (which would be also usefull ie. for undo/redo feature).