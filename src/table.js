import murmurhash  from 'murmurhash'
import {FractionalIndex, Entry, isHigher, generateKey, MIN_SEQ, MAX_SEQ} from './seq.js'

let latestClock = 0
const CLOCK_MASK = ~((1 << 4) - 1)

/**
 * Key used by fractional index entries.
 * @typedef {number[]} FractionalKey
 */


/**
 * Hybrid logical clock - gives a monotonically increasing timestamps.
 * @returns {number}
 * 
 * @see {@link https://www.bartoszsypytkowski.com/hybrid-logical-clocks/}
 */
export const hlc = () => {
    let curr = Date.now() & CLOCK_MASK
    latestClock = Math.max(latestClock, curr)
    return ++latestClock
}

/**
 * Alias for Row definition.
 * @typedef {any[]} Row
 */

/**
 * Update type specifying a new series of rows to be inserted or updated.
 * @typedef {{upsertRows:{columns: FractionalKey[], rows: Entry<any>[]}}} UpsertRows
 */

/**
 * Update type specifying a new series of columns to be inserted or updated.
 * @typedef {{upsertColumns:{columns: FractionalKey[]}}} UpsertColumns
 */

/**
 * Update type specifying a series of rows to be fully deleted.
 * @typedef {{deleteRows:{rows: FractionalKey[]}}} UpsertColumns
 */

/**
 * Update type specifying a series of columns to be fully deleted.
 * @typedef {{deleteColumns:{columns: FractionalKey[]}}} UpsertColumns
 */

/**
 * A wrapper around all Table-related update operations.
 * @typedef {{timestamp: number, data: (UpsertRows|UpsertColumns|DeleteRows|DeleteColumns)}} Update
 */

export class Table {
    /**
     * Creates a new Table with new unique identifier.
     * @param {string} id 
     */
    constructor(id) {
        /** 
         * Globally unique peer identifier.
         * @type {string} 
         */
        this.id = id
        /** 
         * Consistent hash of a peer identifier (32 bits).
         * @type {number} 
         */
        this.hashId = murmurhash.v3(id)
        /** 
         * CRDT index for rows.
         * @type {FractionalIndex<null>} 
         */
        this.rowIndex = new FractionalIndex()
        /** 
         * CRDT index for columns.
         * @type {FractionalIndex<null>} 
         */
        this.colIndex = new FractionalIndex()
        /** 
         * User data in 2-dimensional table.
         * @type {Row[]} 
         */
        this.body = []
        /**
         * Last modification timestamps which positions map directly to correcponding cells in `this.body`.
         * @type {(Version|null)[][]}
         */
        this.versions = []

        /**
         * Timestamp provider.
         * @returns {number} timestamp
         */
        this.clock = hlc
        /**
         * Callback invoked whenever a change is produces in result of actions performed on this Table.
         * Can be used ie. to broadcast changes to remote peers.
         * @param {Update} update 
         */
        this.onUpdate = (update) => {}
    }

    /**
     * Returns a new Dot version representing new update.
     * @returns {Version}
     */
    incVersion() {
        return { timestamp: this.clock(), origin: this.id }
    }

    /**
     * Returns a row a a given index, or null if there's no row at that index.
     * @param {number} row a row index
     * @returns {Row|null}
     */
    getRow(row) {
        if (index < this.entries) {
            return this.entries[row]
        } else {
            return null
        }
    }

    /**
     * Applies update (possibly comming from remote peer) onto local replica.
     * @param {Update} update 
     */
    apply(update) {
        //console.log(this.id, 'applying', update)
        if (update.upsertColumns) {
            const op = /** @type {UpsertColumns} */ (update.upsertColumns)
            this.applyUpsertColumns(op.columns, update.timestamp)
        }
        if (update.deleteRows) {
            const op = /** @type {DeleteRows} */ (update.deleteRows)
            this.applyDeleteRows(op.rows, update.timestamp)
        }
        if (update.upsertRows) {
            const op = /** @type {UpsertRows} */ (update.upsertRows)
            this.applyUpsertRows(op.columns, op.rows, update.timestamp)
        }
        if (update.deleteColumns) {
            const op = /** @type {DeleteColumns} */ (update.deleteColumns)
            this.applyDeleteColumns(op.columns, update.timestamp)
        }
    }

    /**
     * @private
     * @param {FractionalKey[]} columns
     * @param {Version} version
     * @returns {number[]}
     */
    applyUpsertColumns(columns, version) {
        const result = []
        for (let col of columns) {
            let f = this.colIndex.getIndexByKey(col)
            if (f.tombstoned && isHigher(f.tombstoned, version)) {
                // column was tombstoned and has higher version than current index, skip
                result.push(-1)
                continue
            }

            if (!f.found) {
                let index = f.index
                this.colIndex.insert(index, col, null)
                // adjust all existing rows
                for (let i = 0; i < this.body.length; i++) {
                    this.body[i].splice(index, 0, null)
                    this.versions[i].splice(index, 0, null)
                }
            }
            result.push(f.index)
        }
        return result
    }

    /**
     * @private
     * @param {FractionalKey[]} columns
     * @param {Entry<any>[]} entries
     * @param {Version} version 
     */
    applyUpsertRows(columns, entries, version) {
        const columnIndexes = this.applyUpsertColumns(columns, version) // normalize columns
        for (let entry of entries) {
            let f = this.rowIndex.getIndexByKey(entry.key)
            if (f.tombstoned && isHigher(f.tombstoned, version)) {
                // this entry was already deleted with a higher timestamp
                continue
            }
            /** @type {Row} */
            let row 
            /** @type {Version[]} */
            let versions
            if (f.found) {
                // this.applyUpsertColumns already adjusted the number of columns in each row
                row = this.body[f.index]
                versions = this.versions[f.index]
            } else {
                let index = f.index
                // create a new 0-initialized rows
                this.rowIndex.insert(index, entry.key, null) //splice(index, 0, new Entry(entry.key, null))
                row = new Array(this.colIndex.length).fill(null)
                versions = new Array(this.colIndex.length).fill(null)
                this.body.splice(index, 0, row)
                this.versions.splice(index, 0, versions)
            }
            // update rows values
            for (let i=0; i < entry.value.length; i++) {
                let newValue = entry.value[i]
                let cellIndex = columnIndexes[i]
                if (cellIndex >= 0) {
                    // cellIndex < 0 means that column was already removed by newer
                    let cellVersion = versions[cellIndex]
                    if (isHigher(version, cellVersion)) {
                        versions[cellIndex] = version
                        row[cellIndex] = newValue
                    }
                }
            }
        }

    }    

    /**
     * @private
     * @param {FractionalKey[]} rows
     * @param {Version} version 
     */
    applyDeleteRows(rows, version) {
        for (let key of rows) {
            let f = this.rowIndex.getIndexByKey(key)
            if (f.found) {
                let rowIndex = f.index
                // check if there are entries with higher version that current delete timestamp
                let row = this.body[rowIndex]
                let versions = this.versions[rowIndex]
                let keep = false
                for (let i=0; i < row.length; i++) {
                    if (isHigher(version, versions[i])) {
                        // deletion timestamp is higher than latest update
                        row[i] = null
                        versions[i] = version
                    } else if (row[i] !== null) { // confirm that this cell was not already deleted
                        // we cannot remove row, there's a newer data around
                        keep = true
                    }
                }

                if (!keep) {
                    // add deleted row to tombstones
                    this.body.splice(rowIndex, 1)
                    this.versions.splice(rowIndex, 1)
                    // remove row from fractional map
                    this.rowIndex.remove(rowIndex)
                    this.rowIndex.tombstone(key, version)
                }
            } else {
                this.rowIndex.tombstone(key, version)
            }
        }
    }    

    /**
     * @private
     * @param {FractionalKey[]} columns
     * @param {Version} version 
     */
    applyDeleteColumns(columns, version) {
        for (let key of columns) {
            let f = this.colIndex.getIndexByKey(key)
            if (f.found) {
                let colIndex = f.index
                let keep = false
                for (let i = 0; i < this.body.length; i++) {
                    let versions = this.versions[i]
                    let row = this.body[i]
                    if (isHigher(version, versions[colIndex])) {
                        // clear cell data responsible for column
                        versions[colIndex] = version
                        row[colIndex] = null
                    } else if (row[colIndex] !== null) { // confirm that this cell was not already deleted
                        // we cannot remove column, there's a newer data around
                        keep = true
                    }
                }
                if (!keep) {
                    // remove column from fractional map
                    this.colIndex.remove(colIndex)
                    this.colIndex.tombstone(key, version)
                    // delete column on all existing rows
                    for (let i = 0; i < this.body.length; i++) {
                        this.body[i].splice(colIndex, 1)
                        this.versions[i].splice(colIndex, 1)
                    }
                }
            } else {
                this.colIndex.tombstone(key, version)
            }
        }
    }    

    /**
     * Inserts a whole set of consecutive columns to a table, starting at a given column `index`.
     * Any following columns will be shifted to the right.
     * 
     * @param {number} index column index where insertion should start
     * @param {number} count number of new columns to insert.
     */
    insertColumns(index, count = 1) {
        let { left, right } = this.colIndex.neighbors(index)
        /** @type {FractionalKey[]} */
        const columns =  []
        for (let i = 0; i < count; i++) {
            const colKey = generateKey(this.hashId, left, right)
            columns.push(colKey)
            left = colKey;
        }
        const update = { timestamp: this.incVersion(), upsertColumns: { columns } }
        this.apply(update)
        this.onUpdate(update)
    }

    /**
     * Inserts a whole new set of consecutive rows to a table, starting at a given row `index`.
     * @param {number} index row index where the insertion should start
     * @param {Row[]} values collection of rows to be inserted
     */
    insertRows(index, values) {
        /** @type {Entry<any>[]} */
        const rows = []
        // check if we don't need to add new columns to accomodate incoming row cells
        const columns = this.adjustColumns(0, values)
        let { left, right } = this.rowIndex.neighbors(index)
        for (let value of values) {
            const rowKey = generateKey(this.hashId, left, right)
            const entry = new Entry(rowKey, value)
            rows.push(entry)
            left = rowKey;
        }
        const update = { timestamp: this.incVersion(), upsertRows: { columns, rows } }
        this.apply(update) //TODO: optimization - don't threat local & remote updates the same way
        this.onUpdate(update)
    }
    
    /**
     * @private 
     * @param {number} start 
     * @param {Row[]} rows 
     * @returns {FractionalKey[]}
     */
    adjustColumns(start, rows) {
        /** @type {FractionalKey[]} */
        const columns = []
        let i = start
        let left = /** @type {FractionalKey} */ (MIN_SEQ)
        for (let row of rows) {
            for (; i < start + row.length; i++) {
                if (i < this.colIndex.length) {
                    left = this.colIndex.entries[i].key
                    columns.push(left)
                } else {
                    let key = generateKey(this.hashId, left, MAX_SEQ)
                    columns.push(key)
                    left = key
                }
            }
        }
        return columns
    }

    /**
     * Upserts block of cells in determined by selection range starting at a given `row` index and `col` index.
     * This method will update values of existing rows found at that following indexes, or insert new rows if
     * no row was found at a matching index.
     * 
     * @param {number} row first row index where updating selection starts
     * @param {number} col first column index where updating selection starts
     * @param {Row[]} values cells of the corresponding rows to be updated. 
     */
    updateCells(row, col, values) {
        // check if we don't need to add new columns to accomodate incoming row cells
        const columns = this.adjustColumns(col, values)
        const rows = []
        let i = row
        let lastRow = this.rowIndex.lastKey
        for (let value of values) {
            /** @type {Entry<any>} */
            let entry
            if (i < this.rowIndex.length) {
                entry = new Entry(this.rowIndex.entries[i].key, value)
            } else {
                let rowKey = generateKey(this.hashId, lastRow, /** @type {FractionalKey} */ MAX_SEQ)
                entry = new Entry(rowKey, value)
                lastRow = rowKey
            }
            rows.push(entry)
            i++
        }
        const update = {
            timestamp: this.incVersion(),
            upsertRows: { columns, rows }
        }
        this.apply(update) //TODO: optimization - don't thread local & remote updates the same way
        this.onUpdate(update)
    }

    /**
     * Deleted whole series of consecutive rows, starting at a given row `index`.
     * @param {number} index row index where deletion range should start
     * @param {number} length number of rows to delete
     */
    deleteRows(index, length = 1) {
        const rows = []
        const end = Math.min(this.rowIndex.length, index + length)
        for (let i = index; i < end; i++) {
            const row = this.rowIndex.entries[i].key
            rows.push(row)
        }
        const update = { timestamp: this.incVersion(), deleteRows: { rows } }
        this.apply(update) //TODO: optimization - don't thread local & remote updates the same way
        this.onUpdate(update)
    }

    /**
     * Deleted whole series of consecutive columns, starting at a given column `index`.
     * @param {number} index column index where deletion range should start
     * @param {number} length number of columns to delete
     */
    deleteColumns(index, length = 1) {
        const columns = []
        const end = Math.min(this.colIndex.length, index + length)
        for (let i = index; i < end; i++) {
            const col = this.colIndex.entries[i].key
            columns.push(col)
        }
        const update = { timestamp: this.incVersion(), deleteColumns: { columns } }
        this.apply(update) //TODO: optimization - don't thread local & remote updates the same way
        this.onUpdate(update)
    }

    /**
     * Returns a piece of current table matching the boundaries set by a given selection.
     * @param {Selection} selection 
     * @returns {Row[]}
     */
    view(selection) {
        const { upperLeft, lowerRight } = materializeSelection(this, selection)
        const view = []
        for (let i = upperLeft.row; i < lowerRight.row; i++) {
            let row = this.body[i]
            let res = []
            for (let j = upperLeft.col; j < lowerRight.col; j++) {
                res.push(row[j])
            }
            view.push(res)
        }
        return view
    }

    /**
     * 
     * @param {{row:number,col:number}} x 
     * @param {{row:number,col:number}} y
     * @returns {Selection}
     */
    select(x, y) {
        const { upperLeft, lowerRight } = normalizeArea(x, y)
        if (upperLeft.row < 0 || upperLeft.col < 0 || lowerRight.row >= this.rowIndex.length || lowerRight.col >= this.colIndex.length) {
            throw new Error('selected area is outside of the bound of the table')
        }
        const corner1 = {
            row: generateKey(this.hashId, 
                upperLeft.row === 0 ? MIN_SEQ : this.rowIndex.entries[upperLeft.row - 1].key,
                upperLeft.row === this.rowIndex.length ? MAX_SEQ : this.rowIndex.entries[upperLeft.row].key),
            col: generateKey(this.hashId,
                upperLeft.col === 0 ? MIN_SEQ : this.colIndex.entries[upperLeft.col - 1].key,
                upperLeft.col === this.colIndex.length ? MAX_SEQ : this.colIndex.entries[upperLeft.col].key),
        }
        const corner2 = {
            row: generateKey(this.hashId,
                lowerRight.row === this.rowIndex.length ? MIN_SEQ : this.rowIndex.entries[lowerRight.row].key,
                lowerRight.row + 1 >= this.rowIndex.length ? MAX_SEQ : this.rowIndex.entries[lowerRight.row + 1].key),
            col: generateKey(this.hashId,
                lowerRight.col === this.colIndex.length ? MIN_SEQ : this.colIndex.entries[lowerRight.col].key,
                lowerRight.col + 1 >= this.colIndex.length ? MAX_SEQ : this.colIndex.entries[lowerRight.col + 1].key),
        }
        return new Selection(corner1, corner2)
    }
}

/**
 * @typedef {{row:FractionalKey,col:FractionalKey,version:Version}} FractionalPosition
 */

/**
 * Selection represents logical rectangual area within a Table. It maintains position while table structure is being changed.
 */
export class Selection {
    /**
     * @param {FractionalPosition} corner1 
     * @param {FractionalPosition} corner2 
     */
    constructor(corner1, corner2) {
        /** @type {FractionalPosition} */
        this.corner1 = corner1
        /** @type {FractionalPosition} */
        this.corner2 = corner2
    }
}

/**
 * Materializes fractional position into pair of (row, column) indexes matching that position coordinates.
 * 
 * @param {Table} table 
 * @param {FractionalPosition} pos 
 * @returns {{row:number,col:number}}
 */
const materializePosition = (table, pos) => {
    const row = table.rowIndex.getIndexByKey(pos.row)
    const col = table.colIndex.getIndexByKey(pos.col)
    return { row: row.index, col: col.index }
}

/**
 * @param {Table} table 
 * @param {Selection} selection 
 * @returns {{upperLeft:{row:number,col:number}, lowerRight:{row:number,col:number}}}
 */
const materializeSelection = (table, selection) => {
    const x = materializePosition(table, selection.corner1)
    const y = materializePosition(table, selection.corner2)
    return normalizeArea(x, y)
}

/**
 * 
 * @param {{row:number,col:number}} x 
 * @param {{row:number,col:number}} y 
 * @returns {{upperLeft:{row:number,col:number}, lowerRight:{row:number,col:number}}}
 */
const normalizeArea = (x, y) => {
    const upperLeft = {
        row: Math.min(x.row, y.row),
        col: Math.min(x.col, y.col)
    }
    const lowerRight = {
        row: Math.max(x.row, y.row),
        col: Math.max(x.col, y.col)
    }
    return { upperLeft, lowerRight }

}