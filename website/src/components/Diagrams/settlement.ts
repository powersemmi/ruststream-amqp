import type {Box, Lane, Link} from './flow';
import {table} from './table';
import {lines, type Words} from './words';

/** The outcomes of a handler on an at-least-once subscription, in the order the page reads them. */
export const OUTCOMES = ['ack', 'retry', 'retryAfter', 'drop', 'unsettled'] as const;

const EFFECT_KIND: Record<(typeof OUTCOMES)[number], Box['kind']> = {
  ack: 'good',
  retry: 'core',
  retryAfter: 'core',
  drop: 'bad',
  unsettled: 'core',
};

/*
 * The settlement table drawn as a timeline of three columns: what the handler returns, the
 * disposition the subscription sends, and what the broker does with the message.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const drawn = table(words, {
    lanes: ['outcomeLane', 'wireLane', 'brokerLane'],
    widths: [104, 112, 120],
    rows: OUTCOMES.map((outcome) => [
      {id: outcome, code: outcome !== 'unsettled', rows: 3},
      {id: `${outcome}Wire`, code: outcome !== 'unsettled', rows: 3},
      {id: `${outcome}Effect`, kind: EFFECT_KIND[outcome], rows: 3},
    ]),
  });
  const bottom = Math.max(...drawn.boxes.map((box) => box.y + box.h));
  drawn.boxes.push({
    id: 'atMostOnce',
    x: 0,
    y: bottom + 20,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'atMostOnce'),
  });
  return drawn;
}
