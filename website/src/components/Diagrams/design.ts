import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the fe2o3-amqp client, and the broker at the bottom. Each subscription runs on a
 * session of its own; every publisher of a connection shares one session and one sender link
 * per address.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 204, {kind: 'group'}),
    box('subscriber', left, 226, column, 58, {kind: 'broker', code: true}),
    box('publishers', right, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/publishing/',
    }),
    box('message', left, 294, column, 30, {
      kind: 'broker',
      code: true,
      href: '/components/settlement/',
    }),
    box('request', right, 294, column, 30, {kind: 'broker', href: '/components/request-reply/'}),
    box('broker', left, 334, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/broker/',
    }),
    box('descriptor', right, 334, column, 58, {
      kind: 'broker',
      code: true,
      href: '/components/subscriptions/',
    }),
    box('client', 0, 428, 360, 80, {kind: 'group'}),
    box('receiver', left, 454, column, 44, {kind: 'broker', href: '/architecture/lanes/'}),
    box('sender', right, 454, column, 44, {kind: 'broker', href: '/architecture/path/'}),
    box('server', 12, 532, 336, 36, {kind: 'broker', href: '/architecture/amqp/'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'subscriber', to: 'receiver', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'sender', exit: 'right', enter: 'right'},
    {from: 'receiver', to: 'server', enterAt: centre(left)},
    {from: 'sender', to: 'server', enterAt: centre(right)},
  ];
  return {boxes, links};
}
