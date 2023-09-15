import { Table } from '../src/table'

test('Table insert rows (empty table)', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    let expected = [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2']
    ]
    a.insertRows(0, expected)
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table insert rows (prepend)', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a3', 'b3', 'c3'], 
        ['a4', 'b4', 'c4']
    ])
    b.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2']
    ])
    const expected = [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2'], 
        ['a3', 'b3', 'c3'],
        ['a4', 'b4', 'c4']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table insert rows (append)', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2']
    ])
    a.insertRows(2, [
        ['a3', 'b3', 'c3'], 
        ['a4', 'b4', 'c4']
    ])
    const expected = [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2'], 
        ['a3', 'b3', 'c3'],
        ['a4', 'b4', 'c4']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table insert rows (in the middle)', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a4', 'b4', 'c4']
    ])
    b.insertRows(1, [
        ['a2', 'b2', 'c2'], 
        ['a3', 'b3', 'c3']
    ])
    const expected = [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2'], 
        ['a3', 'b3', 'c3'],
        ['a4', 'b4', 'c4']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table delete rows', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2'], 
        ['a3', 'b3', 'c3'],
        ['a4', 'b4', 'c4']
    ])
    b.deleteRows(1, 2)
    const expected = [
        ['a1', 'b1', 'c1'],
        ['a4', 'b4', 'c4']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table insert rows with uneven length', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a4', 'b4', 'c4']
    ])
    b.insertRows(1, [
        ['a2', 'b2'], 
        ['a3', 'b3', 'c3', 'd3']
    ])
    const expected = [
        ['a1', 'b1', 'c1', null], 
        ['a2', 'b2', null, null], 
        ['a3', 'b3', 'c3', 'd3'],
        ['a4', 'b4', 'c4', null]
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table insert columns', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a3', 'b3', 'c3'],
    ])
    a.insertRows(1, [
        ['a2', 'b2', 'c2']
    ])
    b.insertColumns(1, 2)
    const expected = [
        ['a1', null, null, 'b1', 'c1'], 
        ['a2', null, null, 'b2', 'c2'],
        ['a3', null, null, 'b3', 'c3']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table insert columns', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a3', 'b3', 'c3'],
    ])
    a.insertRows(1, [
        ['a2', 'b2', 'c2']
    ])
    b.insertColumns(1, 2)
    const expected = [
        ['a1', null, null, 'b1', 'c1'], 
        ['a2', null, null, 'b2', 'c2'],
        ['a3', null, null, 'b3', 'c3']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table delete columns', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1', 'd1'], 
        ['a3', 'b3', 'c3', 'd3'],
    ])
    a.insertRows(1, [
        ['a2', 'b2', 'c2', 'd2']
    ])
    a.deleteColumns(1, 2)
    const expected = [
        ['a1', 'd1'], 
        ['a2', 'd2'],
        ['a3', 'd3']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table update cells', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1', 'd1'], 
        ['a2', 'b2', 'c2', 'd2'], 
        ['a3', 'b3', 'c3', 'd3'],
        ['a4', 'b4', 'c4', 'd4']
    ])
    a.updateCells(1, 2, [
        [1111, 2222], 
        [3333, 4444]
    ])
    const expected = [
        ['a1', 'b1', 'c1', 'd1'], 
        ['a2', 'b2', 1111, 2222], 
        ['a3', 'b3', 3333, 4444],
        ['a4', 'b4', 'c4', 'd4']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table update cells (with extend)', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2'], 
        ['a3', 'b3', 'c3']
    ])
    a.updateCells(1, 1, [
        [1111, 2222, 3333], 
        [4444, 5555, 6666], 
        [7777, 8888, 9999]
    ])
    const expected = [
        ['a1', 'b1', 'c1', null], 
        ['a2', 1111, 2222, 3333], 
        ['a3', 4444, 5555, 6666],
        [null, 7777, 8888, 9999]
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table concurrent updates', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['a1', 'b1', 'c1'], 
        ['a2', 'b2', 'c2'], 
        ['a3', 'b3', 'c3']
    ])
    b.apply(updateA)
    
    // concurrent updates
    a.updateCells(0, 0, [
        ['AA', 'AA'],
        ['AA', 'AA']
    ])
    b.updateCells(1, 1, [
        ['BB', 'BB'],
        ['BB', 'BB']
    ])
    expect(a.body).toEqual([
        ['AA', 'AA', 'c1'], 
        ['AA', 'AA', 'c2'], 
        ['a3', 'b3', 'c3']
    ])
    expect(b.body).toEqual([
        ['a1', 'b1', 'c1'], 
        ['a2', 'BB', 'BB'], 
        ['a3', 'BB', 'BB']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    // on conflicts B wins (later timestamp)
    const expected = [
        ['AA', 'AA', 'c1'], 
        ['AA', 'BB', 'BB'], 
        ['a3', 'BB', 'BB']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table concurrent inserts', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])
    b.apply(updateA)
    
    // concurrent inserts
    a.insertRows(2, [
        ['A', 'A', 'A'],
        ['A', 'A', 'A']
    ])
    b.insertRows(0, [
        ['B', 'B', 'B'],
        ['B', 'B', 'B']
    ])
    expect(a.body).toEqual([
        ['0', '0', '0'], 
        ['0', '0', '0'],
        ['A', 'A', 'A'],
        ['A', 'A', 'A']
    ])
    expect(b.body).toEqual([
        ['B', 'B', 'B'],
        ['B', 'B', 'B'],
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    // we can expect interleaving issues on inserts done in the same position
    const expected = [
        ['B', 'B', 'B'],
        ['B', 'B', 'B'],
        ['0', '0', '0'], 
        ['0', '0', '0'],
        ['A', 'A', 'A'],
        ['A', 'A', 'A']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table concurrent inserts (interleaving)', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])
    b.apply(updateA)
    
    // concurrent inserts
    a.insertRows(1, [
        ['A', 'A', 'A'],
        ['A', 'A', 'A']
    ])
    b.insertRows(1, [
        ['B', 'B', 'B'],
        ['B', 'B', 'B']
    ])
    expect(a.body).toEqual([
        ['0', '0', '0'], 
        ['A', 'A', 'A'],
        ['A', 'A', 'A'],
        ['0', '0', '0']
    ])
    expect(b.body).toEqual([
        ['0', '0', '0'], 
        ['B', 'B', 'B'],
        ['B', 'B', 'B'],
        ['0', '0', '0']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    // we can expect interleaving issues on inserts done in the same position
    const expected = [
        ['0', '0', '0'], 
        ['A', 'A', 'A'],
        ['B', 'B', 'B'],
        ['A', 'A', 'A'],
        ['B', 'B', 'B'],
        ['0', '0', '0']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table concurrent update + insert column', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['0', '0', '0'], 
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])
    b.apply(updateA)
    
    // concurrent updates: A changes cells while B pushes new column
    a.updateCells(0, 0, [
        ['A', 'A'],
        ['A', 'A']
    ])
    b.insertColumns(1)
    expect(a.body).toEqual([
        ['A', 'A', '0'], 
        ['A', 'A', '0'], 
        ['0', '0', '0']
    ])
    expect(b.body).toEqual([
        ['0', null, '0', '0'], 
        ['0', null, '0', '0'], 
        ['0', null, '0', '0']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    // on conflicts B wins (later timestamp)
    const expected = [
        ['A', null, 'A', '0'], 
        ['A', null, 'A', '0'], 
        ['0', null, '0', '0']
    ]
    expect(a.body).toEqual(expected)
    expect(b.body).toEqual(expected)
})

test('Table concurrent update + delete column', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['0', '0', '0'], 
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])
    b.apply(updateA)
    
    // concurrent updates: A (lower timestamp) changes cells while B deletes column
    a.updateCells(0, 0, [
        ['A', 'A'],
        ['A', 'A']
    ])
    b.deleteColumns(1)
    expect(a.body).toEqual([
        ['A', 'A', '0'], 
        ['A', 'A', '0'], 
        ['0', '0', '0']
    ])
    expect(b.body).toEqual([
        ['0', '0'], 
        ['0', '0'], 
        ['0', '0']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    expect(a.body).toEqual(b.body)
})

test('Table concurrent update + delete column (2)', () => {
    // we need symetric test when update happens "after" concurrent deletion
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['0', '0', '0'], 
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])
    b.apply(updateA)
    
    // concurrent updates: B changes cells while A (lower timestamp) deletes column
    b.updateCells(0, 0, [
        ['B', 'B'],
        ['B', 'B']
    ])
    a.deleteColumns(1)
    expect(b.body).toEqual([
        ['B', 'B', '0'], 
        ['B', 'B', '0'], 
        ['0', '0', '0']
    ])
    expect(a.body).toEqual([
        ['0', '0'], 
        ['0', '0'], 
        ['0', '0']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    expect(a.body).toEqual(b.body)
})

test('Table concurrent update + delete row', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['0', '0', '0'], 
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])
    b.apply(updateA)
    
    // concurrent updates: A (lower timestamp) changes cells while B deletes row
    a.updateCells(0, 0, [
        ['A', 'A'],
        ['A', 'A']
    ])
    b.deleteRows(1)
    expect(a.body).toEqual([
        ['A', 'A', '0'], 
        ['A', 'A', '0'], 
        ['0', '0', '0']
    ])
    expect(b.body).toEqual([
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    expect(a.body).toEqual(b.body)
})

test('Table concurrent update + delete row (2)', () => {
    // we need symetric test when update happens "after" concurrent deletion
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    a.onUpdate = u => updateA = u
    let updateB
    b.onUpdate = u => updateB = u
    // make syntethic clock on B, which is always later to guarantee determinism
    const clock = a.clock
    b.clock = () => clock() + 10000

    // init table
    a.insertRows(0, [
        ['0', '0', '0'], 
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])
    b.apply(updateA)
    
    // concurrent updates: Bchanges cells while A (lower timestamp) deletes row
    b.updateCells(0, 0, [
        ['B', 'B'],
        ['B', 'B']
    ])
    a.deleteRows(1)
    expect(b.body).toEqual([
        ['B', 'B', '0'], 
        ['B', 'B', '0'], 
        ['0', '0', '0']
    ])
    expect(a.body).toEqual([
        ['0', '0', '0'], 
        ['0', '0', '0']
    ])

    // exchange updates
    a.apply(updateB)
    b.apply(updateA)
    expect(a.body).toEqual(b.body)
})