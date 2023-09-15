# CRDT Table

This is proof of concept for Conflict-free Replicated Data Type representing 2-dimensional table. Planned:

- [x] Insert or update existing rows (conflict resolution timestamp per cell basis)
- [x] Insert new series of rows in between existing rows.
- [x] Automatically add new columns when necessary to accomodate row cell expanding beyond current column count.
- [x] Insert new columns in between existing ones.
- [x] Delete entire rows.
- [x] Delete entire columns.
- Selections - conflict-free projections of part of table:
    - [x] Create selection that maintains its boundaries over added/removed columns.
    - [x] Get part of the data correlated to given selection.
    - [ ] Resize boundaries of selection.
    - [ ] Delete selection.
    - [ ] Update user metadata attached to selection object.
    - [ ] Delete user metadata attached to selection object.
    - [ ] Find all selections intersecting with a given one in scope of a table.
    - [ ] Observe all changes happening within the scope of a selection.

## Selections

Selections represents focused areas within a table/grid ie. in expression `=SUM(H2:L7)` the `H2:L7` represents a selected area. In this PoC selections defined by user API using similar `{row:number,col:number}` pairs of oposite corners, however as row and column numbers can change under concurrent updates, the internal representation of selection uses fractional keys (keys used internally by LSeq to establish order). 

This allows us not only to preciselly define the area that will grow or shirk automatically as rows and columns are inserted or deleted, but thanks to the nature of fractional keys, we can preciselly tell if we want to target boundary before or after the row number (or even the entirety of the row or column).

Selections can be used for many purposes:

- As arguments in formulas.
- For applying cell metadata ie. formatting attributes.
- For representing a merged cells.
- When we only want to render fragment of the table we currently present on the screen (for large tables).

### Observing changes

While this is not implemented atm. selections have also another nice property. Quite often we need to react on changes happening within the boundaries of the selection ie. `=SUM(H2:L7)` in the example above needs to be recomputed whenever a cell within a selection changes its value. This requires efficient way to find selections that contain given cell or intserect with range of changed cells (ie. when we copy paste multiple rows/columns).

For small number of selection brute force over all known selections wouldn't be an issue. For bigger number, we could modify indexing structures for 2-dimensional data such as [R-Trees](https://www.bartoszsypytkowski.com/r-tree/) (used ie. to index geospatial data).

## Conflict resolution details

Row/column inserts are resolved using [LSeq](https://www.bartoszsypytkowski.com/operation-based-crdts-arrays-1/#lseq) - while it has interleaving issues, we believe its not that important on that field as when it comes to ie. collaborative text editing. 

Cell updates and cell updates against row/column deletions are resolved using Last-Write-Wins semantics.

There's a minimal overhead related to keeping tombstone information when entire rows/columns are deleted. This could also be shifted to partially ordered log for keeping info about commutative updates.

### Versioning

This implementation uses a combination `{HybridLogicalTimestamp,PeerID}` as a means of conflict resolution, which on positive side is quite lightweight, but otherwise is pretty cumbersome:

- possible clock drifts on client devices
- no ability to recognize concurrent updates ie. conflict resolution for things like update cell / delete column now depends on the clock timestamp
 
 Easiest proposal is to switch to vector versions, with optional clock timestamps where useful, but they can be quite heavy (especially if used on per cell basis). Another proposal is to use partially-ordered log to detect concurrent conflicts (which would be also usefull ie. for undo/redo feature).