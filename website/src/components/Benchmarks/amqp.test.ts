import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {AMQP, amqp} from './amqp';
import {SCHEMAS, environmentRows, tabContent, type Labels, type Results} from './model';

// The document as the benchmark recipe wrote it, read apart from the module under test, so a
// test compares what the page shows with what the file says.
const document = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../../../docs/benchmarks/results.json'), 'utf8'),
) as Results;

// Every word is its own key, so the tests read the rules and never a page's wording.
const labels = new Proxy({}, {get: (_, key) => String(key)}) as Labels;

describe("this crate's results", () => {
  it('are the document committed beside the pages, under the catalog entry of this crate', () => {
    expect(amqp.broker).toBe(AMQP);
    expect(AMQP.repo).toBe('ruststream-amqp');
    expect(amqp.results).toEqual(document);
  });

  it('declare a schema the model renders', () => {
    expect(SCHEMAS).toContain(amqp.results!.schema);
  });

  it('draw one comparison of every scenario the document measured, in its order', () => {
    const {comparison, code, pending} = tabContent(amqp, 'en', labels);
    expect(comparison).toHaveLength(1);
    expect(comparison[0]!.rows.map((row) => row.scenario)).toEqual(
      document.scenarios!.map((scenario) => scenario.name),
    );
    expect(code!.rows.map((row) => row.scenario)).toEqual(
      document.code!.map((scenario) => scenario.name),
    );
    expect(pending).toBeNull();
  });
});

describe('the machine the numbers were taken on', () => {
  it('lists every field of the document, in its order and as recorded', () => {
    const rows = environmentRows(document.environment!, {});
    expect(rows.map((row) => row.field)).toEqual(Object.keys(document.environment!));
    expect(rows.map((row) => row.value)).toEqual(Object.values(document.environment!));
  });

  it('keeps the fields the broker-bound arithmetic and the build need', () => {
    const fields = environmentRows(amqp.results!.environment!, {}).map((row) => row.field);
    expect(fields).toEqual(
      expect.arrayContaining(['round_trip', 'rustflags', 'profile', 'broker']),
    );
  });
});
