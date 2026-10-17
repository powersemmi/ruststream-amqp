import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

const from = (id: string): string[] =>
  retries(words)
    .links.filter((link) => link.from === id)
    .map((link) => link.to);

it('decides on the attempt cap before a retry goes anywhere', () => {
  expect(from('outcome')).toEqual(['cap']);
  expect(from('cap')).toEqual(['requeue', 'copy', 'deadLetter', 'rejected']);
});

it('brings back only what is below the cap, to the subscription address', () => {
  expect(from('requeue')).toEqual(['returns']);
  expect(from('copy')).toEqual(['returns']);
  // At the cap the message leaves the subscription: nothing links back.
  expect([...from('deadLetter'), ...from('rejected')]).toEqual([]);
});
