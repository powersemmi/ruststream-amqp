import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one delivery that is answered, from the broker's transfer to its disposition. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'transfer', kind: 'broker', href: '/architecture/amqp/transfers/'},
    {key: 'pump', kind: 'broker', rows: 3},
    {key: 'channel', kind: 'broker', href: '/architecture/lanes/'},
    {key: 'dispatch', kind: 'core', href: '/architecture/lanes/'},
    {key: 'decode'},
    {key: 'body', kind: 'service'},
    {key: 'encode'},
    {key: 'publisher', kind: 'broker', rows: 3},
    {key: 'settle', kind: 'core', href: '/architecture/settlement/'},
    {key: 'disposition', kind: 'broker', href: '/architecture/amqp/transfers/'},
  ]);
}
