import { Table } from '../src/table'

test('Table select basic', () => {
    const a = new Table('A')
    const b = new Table('B')
    a.onUpdate = u => b.apply(u)
    b.onUpdate = u => a.apply(u)

    a.insertRows(0, [
        ['a1', 'b1', 'c1', 'd1'], 
        ['a2', 'b2', 'c2', 'd2'], 
        ['a3', 'b3', 'c3', 'd3'], 
    ])

    const s = b.select({row:0,col:2},{row:2,col:3})
    let view = b.view(s)
    expect(view).toEqual([
        ['c1', 'd1'], 
        ['c2', 'd2'], 
        ['c3', 'd3']
    ])
})

test('Table select add column', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    let updateB
    a.onUpdate = u => updateA = u
    b.onUpdate = u => updateB = u

    // init table
    a.insertRows(0, [
        ['a1', 'b1', 'c1', 'd1'], 
        ['a2', 'b2', 'c2', 'd2'], 
        ['a3', 'b3', 'c3', 'd3'], 
    ])
    b.apply(updateA)

    // concurrently create a selection and insert row affecting it
    const s = b.select({row:0,col:2},{row:2,col:3})
    let view = b.view(s)
    expect(view).toEqual([
        ['c1', 'd1'], 
        ['c2', 'd2'], 
        ['c3', 'd3']
    ])

    a.insertColumns(3)
    expect(a.body).toEqual([
        ['a1', 'b1', 'c1', null, 'd1'], 
        ['a2', 'b2', 'c2', null, 'd2'], 
        ['a3', 'b3', 'c3', null, 'd3'], 
    ])
    b.apply(updateA)
    view = b.view(s)
    expect(view).toEqual([
        ['c1', null, 'd1'], 
        ['c2', null, 'd2'], 
        ['c3', null, 'd3']
    ])
    expect(view).toEqual(a.view(s))
})

test('Table select add row', () => {
    const a = new Table('A')
    const b = new Table('B')
    let updateA
    let updateB
    a.onUpdate = u => updateA = u
    b.onUpdate = u => updateB = u

    // init table
    a.insertRows(0, [
        ['a1', 'b1', 'c1', 'd1'], 
        ['a2', 'b2', 'c2', 'd2'], 
        ['a4', 'b4', 'c4', 'd4']
    ])
    b.apply(updateA)

    // concurrently create a selection and insert row affecting it
    const s = b.select({row:0,col:2},{row:2,col:3})
    let view = b.view(s)
    expect(view).toEqual([
        ['c1', 'd1'], 
        ['c2', 'd2'], 
        ['c4', 'd4']
    ])

    a.insertRows(2, [['a3', 'b3', 'c3', 'd3']])
    expect(a.body).toEqual([
        ['a1', 'b1', 'c1', 'd1'], 
        ['a2', 'b2', 'c2', 'd2'], 
        ['a3', 'b3', 'c3', 'd3'], 
        ['a4', 'b4', 'c4', 'd4']
    ])
    b.apply(updateA)
    view = b.view(s)
    expect(view).toEqual([
        ['c1', 'd1'], 
        ['c2', 'd2'], 
        ['c3', 'd3'],
        ['c4', 'd4']
    ])
    expect(view).toEqual(a.view(s))
})
