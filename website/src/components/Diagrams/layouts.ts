import {addresses} from './addresses';
import {connection} from './connection';
import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {path} from './path';
import {retries} from './retries';
import {settlement} from './settlement';
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
  design,
  lifecycle,
  path,
  settlement,
  retries,
  lanes,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;

/** The boxes and links of the diagram `name`, with the words of the page. */
export function layoutOf(name: DiagramName, words: Words): Layout {
  return LAYOUTS[name](words);
}
