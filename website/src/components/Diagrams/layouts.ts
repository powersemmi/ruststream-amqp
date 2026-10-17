import {addresses} from './addresses';
import {connection} from './connection';
import type {Box, Lane, Link} from './flow';
import {transactions} from './transactions';
import {outcomes, transfers} from './transfers';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  connection,
  addresses,
  transfers,
  outcomes,
  transactions,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;

/** The boxes and links of the diagram `name`, with the words of the page. */
export function layoutOf(name: DiagramName, words: Words): Layout {
  return LAYOUTS[name](words);
}
