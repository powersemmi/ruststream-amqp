import type {Box, Lane, Link} from './flow';
import {table} from './table';
import {lines, word, type Words} from './words';

// The columns of the timeline: the service's receiver and the broker's sender.
const COLUMNS = {receiver: 82, sender: 278} as const;
const W = 148;

/*
 * Link credit as a timeline: the receiver grants credit, the sender transfers until it is used
 * up, and the client grants it again once half of what arrived is settled.
 */
export function transfers(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const step = (
    id: string,
    column: keyof typeof COLUMNS,
    y: number,
    fields: Partial<Box> = {},
  ): Box => ({
    id,
    x: COLUMNS[column] - W / 2,
    y,
    w: W,
    h: 44,
    kind: column === 'receiver' ? 'service' : 'broker',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    step('grant', 'receiver', 28),
    step('send', 'sender', 100),
    step('settle', 'receiver', 172),
    step('refill', 'receiver', 244),
    step('resume', 'sender', 316),
    {id: 'rule', x: 0, y: 384, w: 360, h: 44, kind: 'note', lines: lines(words, 'rule')},
  ];
  const links: Link[] = [
    {from: 'grant', to: 'send', label: word(words, 'flow')},
    {from: 'send', to: 'settle', label: word(words, 'transfer')},
    {from: 'settle', to: 'refill', label: word(words, 'half')},
    {from: 'refill', to: 'resume', label: word(words, 'flow')},
  ];
  const lanes: Lane[] = [
    {x: COLUMNS.receiver, label: word(words, 'receiverLane')},
    {x: COLUMNS.sender, label: word(words, 'senderLane')},
  ];
  return {boxes, links, lanes};
}

/** The outcomes a disposition can carry, in the order the specification defines them. */
export const OUTCOMES = ['accepted', 'rejected', 'released', 'modified'] as const;

const EFFECT_KIND = {
  accepted: 'good',
  rejected: 'bad',
  released: 'core',
  modified: 'core',
} as const;

/*
 * The four outcomes of a disposition and what each leaves the message as on the broker, with the
 * settle modes under them.
 */
export function outcomes(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const drawn = table(words, {
    lanes: ['outcomeLane', 'brokerLane'],
    widths: [124, 212],
    rows: OUTCOMES.map((outcome) => [
      {id: outcome, kind: 'service', code: true},
      {id: `${outcome}Effect`, kind: EFFECT_KIND[outcome]},
    ]),
  });
  const bottom = Math.max(...drawn.boxes.map((box) => box.y + box.h));
  drawn.boxes.push({
    id: 'modes',
    x: 0,
    y: bottom + 20,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'modes'),
  });
  return drawn;
}
