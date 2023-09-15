import murmurhash  from 'murmurhash'

let latestClock = 0
const CLOCK_MASK = ~((1 << 4) - 1)

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
 * Lamport clock used to mark changes.
 * @typedef {{timestamp:number,origin:string}} Version
 */

/**
 * Key used by fractional index entries.
 * @typedef {number[]} FractionalKey
 */

/**
 * Sorted map (ordered by FractionalKey within its entries).
 * Important property is that entries can be indexed with 0-based indexes.
 * 
 * @template T
 * @typedef {Entry<T>[]} FractionalMap<T>
 */

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
         * @type {FractionalMap<null>} 
         */
        this.rowIndex = []
        /** 
         * CRDT index for columns.
         * @type {FractionalMap<null>} 
         */
        this.colIndex = []        
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
         * Tombstones of removed rows and columns.
         * @type {{rows: FractionalMap<Version>, columns: FractionalMap<Version>}}
         */
        this.tombstones = {
            rows: [],   // fractional map of deleted rows
            columns: [] // fractional map of deleted columns
        }

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
            let { index, found } = binarySearch(this.colIndex, col, 0, this.colIndex.length - 1)
            if (!found) {
                if (isTombstoned(this.tombstones.columns, col, version)) {
                    // column was tombstoned and has higher version than current index, skip
                    result.push(-1)
                    continue
                }

                this.colIndex.splice(index, 0, new Entry(col, null))
                // adjust all existing rows
                for (let i = 0; i < this.body.length; i++) {
                    this.body[i].splice(index, 0, null)
                    this.versions[i].splice(index, 0, null)
                }
            }
            result.push(index)
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
            let { index, found } = binarySearch(this.rowIndex, entry.key, 0, this.rowIndex.length - 1)
            /** @type {Row} */
            let row 
            /** @type {Version[]} */
            let versions
            if (found) {
                // this.applyUpsertColumns already adjusted the number of columns in each row
                row = this.body[index]
                versions = this.versions[index]
            } else {
                if (isTombstoned(this.tombstones.rows, entry.key, version)) {
                    // this entry was already deleted with a higher timestamp
                    continue
                }
                // create a new 0-initialized rows
                this.rowIndex.splice(index, 0, new Entry(entry.key, null))
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
            let index = indexOfKey(this.rowIndex, key)
            if (index >= 0) {
                // check if there are entries with higher version that current delete timestamp
                let row = this.body[index]
                let versions = this.versions[index]
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
                    this.rowIndex.splice(index, 1)
                    this.body.splice(index, 1)
                    this.versions.splice(index, 1)
                    // add deleted row to tombstones
                    updateTombstone(this.tombstones.rows, key, version)
                }
            } else {
                // check if we need to update tombstone
                updateTombstone(this.tombstones.rows, key, version)
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
            let colIndex = indexOfKey(this.colIndex, key)
            if (colIndex >= 0) {
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
                    this.colIndex.splice(colIndex, 1)
                    updateTombstone(this.tombstones.columns, key, version)
                    // adjust all existing rows
                    for (let i = 0; i < this.body.length; i++) {
                        this.body[i].splice(colIndex, 1)
                        this.versions[i].splice(colIndex, 1)
                    }
                }
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
        let left = /** @type {FractionalKey} */ (index < this.colIndex.length ? this.colIndex[index-1].key : MIN_SEQ)
        const right = /** @type {FractionalKey} */ (index < this.colIndex.length ? this.colIndex[index].key : MAX_SEQ)
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
        const columns = this.colIndex.map(c => c.key)
        let maxRowLength = 0
        for (let value of values) {
            maxRowLength = Math.max(maxRowLength, value.length)
        }
        for (let col = columns.length; col < maxRowLength; col++) {
            // if row has more columns than current known columns length, we need to add more columns
            const lastColumn = /** @type {FractionalKey} */ (columns.length > 0 ? columns[col-1] : MIN_SEQ)
            const columnKey = generateKey(this.hashId, lastColumn, /** @type {FractionalKey} */ MAX_SEQ)
            columns.push(columnKey)
        }
        let left = /** @type {FractionalKey} */ (index > 0 ? this.rowIndex[index-1].key : MIN_SEQ)
        const right = /** @type {FractionalKey} */ (index < this.rowIndex.length ? this.rowIndex[index].key : MAX_SEQ)
        for (let value of values) {
            const rowKey = generateKey(this.hashId, left, right)
            const entry = new Entry(rowKey, value)
            rows.push(entry)
            left = rowKey;
        }
        const update = { timestamp: this.incVersion(), upsertRows: { columns, rows } }
        this.apply(update) //TODO: optimization - don't thread local & remote updates the same way
        this.onUpdate(update)
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
        // first let's calculate affected columns - possibly a new ones may have to be introduced
        /** @type {FractionalKey[]} */
        const columns = []
        let maxRowLength = 0
        for (let value of values) {
            maxRowLength = Math.max(maxRowLength, value.length)
        }
        for (let i = 0; i < maxRowLength; i++) {
            let curr = i + col
            if (curr < this.colIndex.length) {
                columns.push(this.colIndex[curr].key)
            } else {
                // if row has more columns than current known columns length, we need to add more columns
                const lastColumn = /** @type {FractionalKey} */ (columns.length > 0 ? columns[columns.length-1] : MIN_SEQ)
                const columnKey = generateKey(this.hashId, lastColumn, /** @type {FractionalKey} */ MAX_SEQ)
                columns.push(columnKey)
            }
        }
        const rows = []
        let i = row
        let lastRow = /** @type {FractionalKey} */ (this.rowIndex.length > 0 ? this.rowIndex[this.rowIndex.length-1].key : MIN_SEQ)
        for (let value of values) {
            /** @type {Entry<any>} */
            let entry
            if (i < this.rowIndex.length) {
                entry = new Entry(this.rowIndex[i].key, value)
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
            const row = this.rowIndex[i].key
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
            const col = this.colIndex[i].key
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
                upperLeft.row === 0 ? MIN_SEQ : this.rowIndex[upperLeft.row - 1].key,
                upperLeft.row === this.rowIndex.length ? MAX_SEQ : this.rowIndex[upperLeft.row].key),
            col: generateKey(this.hashId,
                upperLeft.col === 0 ? MIN_SEQ : this.colIndex[upperLeft.col - 1].key,
                upperLeft.col === this.colIndex.length ? MAX_SEQ : this.colIndex[upperLeft.col].key),
        }
        const corner2 = {
            row: generateKey(this.hashId,
                lowerRight.row === this.rowIndex.length ? MIN_SEQ : this.rowIndex[lowerRight.row].key,
                lowerRight.row + 1 >= this.rowIndex.length ? MAX_SEQ : this.rowIndex[lowerRight.row + 1].key),
            col: generateKey(this.hashId,
                lowerRight.col === this.colIndex.length ? MIN_SEQ : this.colIndex[lowerRight.col].key,
                lowerRight.col + 1 >= this.colIndex.length ? MAX_SEQ : this.colIndex[lowerRight.col + 1].key),
        }
        return new Selection(corner1, corner2)
    }
}

const MIN_INT = 0n
const MAX_INT = BigInt(Number.MAX_SAFE_INTEGER)
const MASK = ~((1n << 32n) - 1n)
const MIN_SEQ = [MIN_INT]
const MAX_SEQ = [MAX_INT]

/**
 * Logical position identifier of LSeq.
 * 
 * @template T
 */
export class Entry {
    /**
     * 
     * @param {FractionalKey} key
     * @param {T} value
     */
    constructor(key, value) {
        /** @type {FractionalKey} */
        this.key = key
        /** @type {T} */
        this.value = value
    }

    /**
     * @param {T} newValue 
     * @returns {Entry<T>}
     */
    copy(newValue) {
        return new Entry(key, newValue)
    }
}

/**
 * 
 * @param {FractionalKey} a
 * @param {FractionalKey} b
 * @returns {number}
 */
export const compareKeys = (a, b) => {
    // compare sequences
    let i = 0
    for (; i < a.length && i < b.length; i++) {
        const x = a[i] - b[i]
        if (x !== 0) {
            return x
        }
    }
    return a.length - b.length
}

/**
 * Find insert index for a given key.
 * 
 * @param {FractionalMap<T>} map 
 * @param {FractionalKey} key 
 * @param {number} start 
 * @param {number} end 
 * @returns {{index:number,found:bool}}
 */
const binarySearch = (map, key, start, end) => {
    while (start <= end) {
        let m = (start + end) >> 1
        let cmp = compareKeys(map[m].key, key)
        if (cmp === 0) {
            return { index: m, found: true }
        } else if (cmp < 0) {
            start = m + 1
        } else {
            end = m - 1
        }
    }
    return { index: end + 1, found: false }
}
    
/**
 * @param {FractionalMap<T>} map 
 * @param {FractionalKey} key
 * @returns {number}
 */
const indexOfKey = (map, key) => {
    const { index, found } = binarySearch(map, key, 0, map.length - 1)
    return found ? index : -1
}

/**
 * Generates a new unique sequence, that lexically fits between left (lower) and right (higher) sequence.
 * 
 * The generated number is a series of steps. Each step is a 53bit integer (`Number.MAX_SAFE_INTEGER`),
 * where the lower 32-bit is a Murmur3 hash of clientID that produced this step, while upper 21 bits is 
 * sequence to be generated between the left's and right's number at the same depth.
 * 
 * 
 * @param {number} hash MurMur v3 hash of client's ID.
 * @param {FractionalKey|null} left left neighbor key (lower value in lexical sense)
 * @param {FractionalKey|null} right right neighbor key (higher value in lexical sense)
 * @returns {FractionalKey}
 */
export const generateKey = (hash, left, right) => {
    const h = BigInt(hash)
    const lo = left || []
    const hi = right || []
    const result = []
    let depth = 0
    for (;; depth++) {
        let x = BigInt(depth < lo.length ? lo[depth] : (MIN_INT|h))
        let y = BigInt(depth < hi.length ? hi[depth] : MAX_INT)
        if (x === y) {
            result.push(Number(x))
        } else if ((x >> 32n) + 1n >= (y >> 32n)) {
            // continue
            let n = Number(x)
            result.push(n)
        } else {
            // there's a spare space between hi and lo at current position
            // use it and return
            let n = Number(((x + (1n << 32n)) & MASK) | h)
            result.push(n)
            break;
        }
    }
    return result
}

/**
 * Checks if `left` version is higher than `right` one.
 * @param {Version|null} left 
 * @param {Version|null} right 
 * @returns {boolean}
 */
export const isHigher = (left, right) => {
    if (left === null) {
        return false
    }
    if (right === null) {
        return true
    }

    if (left.timestamp > right.timestamp) {
        return true
    } else if (left.timestamp === right.timestamp) {
        return left.origin > right.origin
    }
}

/**
 * @param {FractionalMap<Version>} map 
 * @param {FractionalKey} key 
 * @param {Version} version 
 * @returns {boolean}
 */
const isTombstoned = (map, key, version) => {
    let { index, found } = binarySearch(map, key, 0, map.length - 1)
    return found && isHigher(map[index].value, version)
}

/**
 * 
 * @param {FractionalMap<Version>} map 
 * @param {FractionalKey} key 
 * @param {Version} version 
 */
const updateTombstone = (map, key, version) => {
    const { index, found } = binarySearch(map, key, 0, map.length - 1)
    if (found) {
        const entry = map[index]
        if (isHigher(version, entry.value)) {
            entry.value = version
        }
    } else {
        map.splice(index, 0, new Entry(key, version))
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
    const row = binarySearch(table.rowIndex, pos.row, 0, table.rowIndex.length - 1)
    const col = binarySearch(table.colIndex, pos.col, 0, table.colIndex.length - 1)
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