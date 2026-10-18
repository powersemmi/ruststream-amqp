import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * What an address holds and how its receivers share it: one queue whose receivers compete for
 * each message, one queue per subscription so that each receiver gets a copy, and a node the
 * broker creates when a receiver asks for a dynamic source.
 */
export function addresses(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  const left = 12;
  const right = 188;
  const column = 160;
  const boxes: Box[] = [
    // Anycast: one queue, two receivers taking turns; each takes its message straight from
    // above, so the words on the two links stand clear of each other.
    box('anycast', 0, 0, 360, 76, {kind: 'group'}),
    box('queue', 12, 26, 336, 40, {kind: 'broker'}),
    box('worker1', left, 108, column, 44, {kind: 'service'}),
    box('worker2', right, 108, column, 44, {kind: 'service'}),
    // Multicast: a queue per subscription, each receiving every message.
    box('multicast', 0, 184, 360, 76, {kind: 'group'}),
    box('copy1', left, 210, column, 40, {kind: 'broker'}),
    box('copy2', right, 210, column, 40, {kind: 'broker'}),
    box('audit', left, 292, column, 44, {kind: 'service'}),
    box('billing', right, 292, column, 44, {kind: 'service'}),
    // Dynamic: the broker creates the node and names it. The gap between the two leaves room
    // for the words on the links across it.
    box('dynamic', 0, 368, 360, 92, {kind: 'group'}),
    box('requester', left, 394, 136, 56, {kind: 'service'}),
    box('temporary', 212, 394, 136, 56, {kind: 'broker'}),
  ];
  // Where along the queue a link leaves, straight above the centre of the column at `x`.
  const above = (x: number): number => (x + column / 2 - 12) / 336;
  const links: Link[] = [
    {from: 'queue', to: 'worker1', exitAt: above(left), label: word(words, 'first')},
    {from: 'queue', to: 'worker2', exitAt: above(right), label: word(words, 'second')},
    {from: 'copy1', to: 'audit', label: word(words, 'every')},
    {from: 'copy2', to: 'billing', label: word(words, 'every')},
    {
      from: 'requester',
      to: 'temporary',
      exit: 'right',
      enter: 'left',
      exitAt: 0.3,
      enterAt: 0.3,
      label: word(words, 'attach'),
    },
    {
      from: 'temporary',
      to: 'requester',
      exit: 'left',
      enter: 'right',
      exitAt: 0.75,
      enterAt: 0.75,
      dashed: true,
      label: word(words, 'named'),
    },
  ];
  return {boxes, links};
}
