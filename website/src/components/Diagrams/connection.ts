import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The AMQP 1.0 model as this crate opens it: one connection between two containers, a session of
 * its own for the publishers and for each subscription, and in each session links that move
 * messages one way between a terminus on the service's side and a node in the broker.
 */
export function connection(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // Two sessions side by side; each column holds its session's window and its link, and the
  // node the link reaches stands straight under it.
  const left = 24;
  const right = 198;
  const column = 138;
  const boxes: Box[] = [
    box('service', 12, 0, 336, 44, {kind: 'service'}),
    box('connection', 0, 76, 360, 240, {kind: 'group'}),
    box('publishers', 12, 102, 162, 202, {kind: 'group'}),
    box('publisherWindow', left, 128, column, 44, {kind: 'core'}),
    box('sender', left, 196, column, 59, {kind: 'service'}),
    box('subscription', 186, 102, 162, 202, {kind: 'group'}),
    box('subscriptionWindow', right, 128, column, 44, {kind: 'core'}),
    box('receiver', right, 196, column, 59, {kind: 'service'}),
    box('broker', 0, 348, 360, 80, {kind: 'group'}),
    box('receipts', left, 374, column, 44, {kind: 'broker'}),
    box('orders', right, 374, column, 44, {kind: 'broker'}),
    box('names', 0, 444, 360, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'service', to: 'connection', label: word(words, 'opens')},
    {from: 'sender', to: 'receipts', label: word(words, 'transfers')},
    {from: 'orders', to: 'receiver', label: word(words, 'transfers')},
  ];
  return {boxes, links};
}
