import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const CELL = 84;
const STEP = 92;

/*
 * A retry on AMQP 1.0: the runtime reads the attempt off the delivery and the registration's cap
 * decides. Below the cap a retry() goes back on the queue with a modified disposition and a
 * retry_after leaves a copy for later; at the cap the message goes to the dead-letter address or
 * is rejected to the broker's own policy.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const answers = ['requeue', 'copy', 'deadLetter', 'rejected'] as const;
  const boxes: Box[] = [
    {id: 'delivery', x: 12, y: 0, w: 336, h: 44, kind: 'broker', lines: lines(words, 'delivery')},
    {id: 'outcome', x: 12, y: 72, w: 336, h: 44, kind: 'service', lines: lines(words, 'outcome')},
    {id: 'cap', x: 12, y: 144, w: 336, h: 44, kind: 'core', lines: lines(words, 'cap')},
    ...answers.map(
      (id, index): Box => ({
        id,
        x: index * STEP,
        y: 220,
        w: CELL,
        h: 104,
        kind: index < 2 ? 'step' : 'bad',
        lines: lines(words, id),
      }),
    ),
    {
      id: 'returns',
      x: 0,
      y: 352,
      w: STEP + CELL,
      h: 58,
      kind: 'broker',
      lines: lines(words, 'returns'),
    },
    {id: 'crash', x: 0, y: 432, w: 360, h: 44, kind: 'note', lines: lines(words, 'crash')},
  ];
  // Each answer is reached straight down from the cap, from above its own column.
  const along = (index: number): number => (index * STEP + CELL / 2 - 12) / 336;
  const links: Link[] = [
    {from: 'delivery', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    ...answers.map((id, index) => ({
      from: 'cap',
      to: id,
      exitAt: along(index),
      ...(index === 0 ? {label: word(words, 'below')} : {}),
      ...(index === 2 ? {label: word(words, 'atCap')} : {}),
    })),
    {from: 'requeue', to: 'returns', enterAt: (CELL / 2) / (STEP + CELL)},
    {from: 'copy', to: 'returns', enterAt: (STEP + CELL / 2) / (STEP + CELL)},
  ];
  return {boxes, links};
}
