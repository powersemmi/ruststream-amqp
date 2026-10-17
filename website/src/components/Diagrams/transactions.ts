import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Transactional posting: a controller declares a transaction at the coordinator, transfers on
 * ordinary sender links carry its id, and the discharge either makes them visible together or
 * discards them all.
 */
export function transactions(words: Words): {boxes: Box[]; links: Link[]} {
  const main = (id: string, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: 12,
    y,
    w: 200,
    h: 48,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    main('declare', 0, {kind: 'service'}),
    main('declared', 74, {kind: 'broker'}),
    main('post', 148),
    main('commit', 222, {kind: 'core'}),
    main('visible', 296, {kind: 'good', h: 62}),
    {
      id: 'rollback',
      x: 236,
      y: 222,
      w: 120,
      h: 136,
      kind: 'bad',
      lines: lines(words, 'rollback'),
    },
    {
      id: 'hidden',
      x: 0,
      y: 382,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'hidden'),
    },
  ];
  const links: Link[] = [
    {from: 'declare', to: 'declared', label: word(words, 'coordinator')},
    {from: 'declared', to: 'post', label: word(words, 'txnId')},
    {from: 'post', to: 'commit'},
    {from: 'commit', to: 'visible', label: word(words, 'accepted')},
    {from: 'post', to: 'rollback', exit: 'right', enter: 'top', label: word(words, 'orElse')},
  ];
  return {boxes, links};
}
