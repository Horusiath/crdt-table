import { compareKeys, generateKey } from "../src";

const A = 0
const B = 1

const parse = (str) => {
    if (str === null) {
        return null
    }
    let res = []
    let parts = str.split(':')
    for (let p of parts) {
        let s = BigInt(parseInt(p[0]))
        let h = BigInt(p.charCodeAt(1) - 65)
        let n = Number((s << BigInt(32)) | h) 
        res.push(n)
    }
    return res
}

const format = (key) => {
    var result = ''
    for (let num of key) {
        let n = BigInt(num)
        let h = Number(n & (1n << 32n) - 1n)
        let s = Number(n >> 32n)
        result += ':' + s.toString() + String.fromCharCode(h + 65)
    }
    return result.substring(1)
}

const testSequence = (client, left, right, expected) => {
    test(`between '${left}' and '${right}' should be '${expected}'`, () => {
        const l = parse(left)
        const r = parse(right)
        const e = parse(expected)
        const actual = generateKey(client, l, r)
        expect(format(actual)).toEqual(expected)
        if (l) expect(compareKeys(actual, l)).toBeGreaterThan(0);
        if (r) expect(compareKeys(actual, r)).toBeLessThan(0);
    })
}

test('sequence gen utility method', () => {
    expect(parse('1B:3C')).toEqual([0x0000000100000001,0x0000000300000002])
})

testSequence(A, null, null, '1A')
testSequence(A, '0A', '1A', '0A:1A')
testSequence(A, '0A', '2A', '1A')
testSequence(A, '0A', '0A:1A', '0A:0A:1A')
// no free space in between to expand
testSequence(A, '0A', '0B', '0A:1A')
testSequence(B, '0A', '0B', '0A:1B')
testSequence(A, '0A', '1B', '0A:1A')
testSequence(B, '0A', '1B', '0A:1B')
// free space between
testSequence(A, '0A', '2B', '1A')
testSequence(B, '0A', '2B', '1B')
// need to descend
testSequence(A, '0A', '0A:1B', '0A:0A:1A')
testSequence(B, '0A', '0A:1B', '0A:0B:1B')

/**
 * Asserts that FractionalKey entries have keys in order matching the entries position.
 * 
 * @param {FractionalKey} seq 
 */
const assertKeysOrder = (seq) => {
    let prev = seq.entries[0].key
    expect(seq.indexOfKey(prev)).toBe(0)
    for (let i = 1; i < seq.entries.length; i++) {
        let curr = seq.entries[i].key
        expect(seq.indexOfKey(curr)).toBe(i)
        expect(compareKeys(prev, curr)).toBeLessThan(0)
        prev = curr
    }
}