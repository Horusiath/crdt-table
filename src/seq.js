const MIN_INT = 0n
const MAX_INT = BigInt(Number.MAX_SAFE_INTEGER)
const MASK = ~((1n << 32n) - 1n)
export const MIN_SEQ = [MIN_INT]
export const MAX_SEQ = [MAX_INT]

/**
 * Lamport clock used to mark changes.
 * @typedef {{timestamp:number,origin:string}} Version
 */

/**
 * Key used by fractional index entries.
 * @typedef {number[]} FractionalKey
 */

/**
 * @template T type of value stored in LSeq
 */
export class FractionalIndex {
    constructor() {
        /**
         * CRDT index for alive entries.
         * @type {Entry<T>[]}
         */
        this.entries = []
        /**
         * CRDT index for tombstoned entries.
         * @type {Entry<Version>[]}
         */
        this.tombstones = []
    }

    /**
     * Returns a length of the vector of alive elements.
     * @returns {number}
     */
    get length() {
        return this.entries.length
    }

    /**
     * @returns {FractionalKey}
     */
    get lastKey() {
        if (this.entries.length === 0) {
            return MIN_SEQ
        } else {
            return this.entries[this.entries.length-1].key
        }
    }

    /**
     * Returns an index under which a corresponding fractional key can be found.
     * Returned structure can be either:
     * - {index,found:true} - key exists in a current map and can be found at given index.
     * - {index,found:false} - key was never seen before, but if you want to insert it, do it at given index.
     * - {tombstoned:version} - key was tombstoned at given version.
     * 
     * @param {FractionalKey} key 
     * @returns {{index:number,found:boolean}|{tombstoned:Version}}
     */
    getIndexByKey(key) {
        const e = binarySearch(this.entries, key, 0, this.entries.length - 1)
        if (!e.found) {
            const { index, found } = binarySearch(this.tombstones, key, 0, this.tombstones.length - 1)
            if (found) {
                return { index: e.index, tombstoned: this.tombstones[index].value }
            }
        }
        return e
    }

    /**
     * Returns fractional keys on the left and right of the given index.
     * 
     * @param {number} index
     * @returns {{left:FractionalKey,right:FractionalKey}}
     */
    neighbors(index) {
        let left = /** @type {FractionalKey} */ (index > 0 && index <= this.entries.length ? this.entries[index-1].key : MIN_SEQ)
        const right = /** @type {FractionalKey} */ (index < this.entries.length ? this.entries[index].key : MAX_SEQ)
        return { left, right }
    }

    /**
     * 
     * @param {number} index 
     * @param {FractionalKey} key 
     * @param {T} value 
     */
    insert(index, key, value) {
        this.entries.splice(index, 0, new Entry(key, value))
    }

    /**
     * 
     * @param {number} index 
     */
    remove(index) {
        this.entries.splice(index, 1)
    }

    /**
     * 
     * @param {FractionalKey} key 
     * @param {Version} version 
     */
    tombstone(key, version) {
        const e = binarySearch(this.tombstones, key, 0, this.tombstones.length - 1)
        if (e.found) {
            const tombstone = this.tombstones[e.index]
            if (isHigher(version, tombstone.value)) {
                tombstone.value = version
            }
        } else {
            this.tombstones.splice(e.index, 0, new Entry(key, version))
        }
    }
}

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