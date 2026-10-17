import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const LANE_X = [12, 188] as const;

/*
 * One subscription's dispatch: the broker's queue sends against the link's credit, the pump task
 * hands the deliveries to a channel bounded at that credit, the stream yields them one at a time,
 * and `workers(n, by_key)` keys each delivery to a lane by its group-id.
 */
export function lanes(words: Words): {boxes: Box[]; links: Link[]} {
  const row = (id: string, y: number, kind: Box['kind'], href?: string): Box => ({
    id,
    x: 12,
    y,
    w: 336,
    h: 44,
    kind,
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    row('queue', 0, 'broker', '/architecture/amqp/addresses/'),
    row('link', 72, 'broker', '/architecture/amqp/transfers/'),
    row('pump', 144, 'broker'),
    row('stream', 216, 'broker'),
    row('keyed', 288, 'core'),
    ...LANE_X.map(
      (x, lane): Box => ({
        id: `lane${lane}`,
        x,
        y: 364,
        w: 160,
        h: 58,
        kind: 'service',
        lines: lines(words, `lane${lane}`),
      }),
    ),
    {
      id: 'replicas',
      x: 0,
      y: 446,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'replicas'),
    },
  ];
  const links: Link[] = [
    {from: 'queue', to: 'link', label: word(words, 'transfers')},
    {from: 'link', to: 'pump', label: word(words, 'received')},
    {from: 'pump', to: 'stream', label: word(words, 'bounded')},
    {from: 'stream', to: 'keyed', label: word(words, 'oneAtATime')},
    ...LANE_X.map((x, lane) => ({
      from: 'keyed',
      to: `lane${lane}`,
      exitAt: (x + 80 - 12) / 336,
    })),
  ];
  return {boxes, links};
}
