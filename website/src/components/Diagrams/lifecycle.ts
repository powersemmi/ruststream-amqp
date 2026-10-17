import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's states as types on the left, and on the right what each transition does on the
 * wire: nothing at construction, the connection and the publishers' session at connect, a session
 * and a receiver per subscription at startup, a sender link on the first publish to an address,
 * and the teardown from the inside out at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, h = 58): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
  });
  const right = (id: string, y: number, h = 58, href?: string): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 490, kind: 'group', lines: lines(words, 'states')},
    {id: 'wire', x: 184, y: 0, w: 176, h: 490, kind: 'group', lines: lines(words, 'wire')},
    left('unconnected', 28),
    left('connected', 196),
    left('closed', 384, 92),
    right('config', 28),
    right('open', 104, 72, '/architecture/amqp/'),
    right('subscriptions', 196, 72, '/architecture/lanes/'),
    right('senders', 288, 72, '/architecture/path/'),
    right('teardown', 384, 92),
    {
      id: 'afterShutdown',
      x: 0,
      y: 512,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'open'},
    {from: 'open', to: 'subscriptions'},
    {from: 'subscriptions', to: 'senders', dashed: true},
    {from: 'senders', to: 'teardown'},
  ];
  return {boxes, links};
}
