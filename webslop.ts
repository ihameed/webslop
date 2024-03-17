type dlist = {container : unknown, next : dlist, prev : dlist}
const dummy_dlist : dlist = {container : null, next : null as unknown as dlist, prev : null as unknown as dlist}
const dlist_prepend = (where : dlist, node : dlist) => {
    const prev = where.prev
    prev.next = node
    node.prev = prev
    node.next = where
    where.prev = node
}
const dlist_unlink = (where : dlist) => {
    const prev = where.prev
    const next = where.next
    prev.next = next
    next.prev = prev
}

export type semaphore = {__semaphore_count : number, __semaphore_waitq : dlist}
type wait_node = Promise<void> & {__wn_waitq : dlist, __wn_wake : () => void}

export const make_semaphore = (count : number) : semaphore => {
    if (count <= 0 || !Number.isSafeInteger(count)) throw new Error(`invalid non-positive or non-integral semaphore count ${count}`)
    const ret = {__semaphore_count : count, __semaphore_waitq : {...dummy_dlist}}
    const waitq = ret.__semaphore_waitq
    waitq.container = ret
    waitq.next = waitq
    waitq.prev = waitq
    return ret
}
export const wait_for_semaphore = (sem : semaphore) : Promise<void> => {
    if (sem.__semaphore_count == 0) {
        let resolve : (val : void) => void = null as any
        const ret = new Promise((res, rej) => { resolve = res; void rej }) as wait_node
        const __wn_waitq : dlist = {...dummy_dlist, container : ret}
        __wn_waitq.next = __wn_waitq
        __wn_waitq.prev = __wn_waitq
        ret.__wn_waitq = __wn_waitq
        ret.__wn_wake = resolve
        dlist_prepend(sem.__semaphore_waitq, ret.__wn_waitq)
        return ret
    }
    --sem.__semaphore_count
    return Promise.resolve()
}
export const release_semaphore = (sem : semaphore) => {
    const wq = sem.__semaphore_waitq
    const next = wq.next
    if (next !== wq) {
        dlist_unlink(next)
        const wn = next.container as wait_node
        queueMicrotask(wn.__wn_wake)
    } else {
        ++sem.__semaphore_count
    }
}
export const with_semaphore = (sem : semaphore) => async <t,> (f : () => t) : Promise<t> => {
    await wait_for_semaphore(sem)
    let r : t
    try {
        r = await f()
    } finally {
        release_semaphore(sem)
    }
    return r
}
export const all_with_limiter = async <t extends readonly (() => unknown)[] | []>(sem : semaphore, xs : t)
    : Promise<{ -readonly [ix in keyof t] : Awaited<ReturnType<t[ix]>> }> =>
{
    const ret = await Promise.all(xs.map(with_semaphore(sem)))
    return ret as any
}
export const all_with_limiter_rec = async <t extends Record<string, () => unknown>>(sem : semaphore, xs : t)
    : Promise<{ -readonly [ix in keyof t] : Awaited<ReturnType<t[ix]>> }> =>
{
    const ret : Record<string, any> = {}
    const tasks = []
    for (const k in xs) {
        const v = xs[k] as () => Promise<any>
        if (v === undefined) continue
        tasks.push(with_semaphore(sem)(v).then(r => ret[k] = r))
    }
    await Promise.all(tasks)
    return ret as any
}
