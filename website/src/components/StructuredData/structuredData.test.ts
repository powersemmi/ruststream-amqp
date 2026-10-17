import {describe, expect, it} from 'vitest';
import {jsonLd, softwareSourceCode} from './structuredData';

describe('softwareSourceCode', () => {
  it('describes the project as the schema.org type for a code repository', () => {
    expect(
      softwareSourceCode({
        description: 'The AMQP 1.0 broker for RustStream.',
        url: 'https://powersemmi.github.io/ruststream-amqp/latest/ru/',
      }),
    ).toEqual({
      '@context': 'https://schema.org',
      '@type': 'SoftwareSourceCode',
      name: 'ruststream-amqp',
      description: 'The AMQP 1.0 broker for RustStream.',
      url: 'https://powersemmi.github.io/ruststream-amqp/latest/ru/',
      codeRepository: 'https://github.com/powersemmi/ruststream-amqp',
      programmingLanguage: 'Rust',
      runtimePlatform: 'tokio',
      license: 'https://www.apache.org/licenses/LICENSE-2.0',
    });
  });
});

describe('jsonLd', () => {
  it('keeps a value from closing the script element it is written into', () => {
    const text = jsonLd({description: 'handlers </script><b>'});
    expect(text).not.toContain('</script>');
    expect(JSON.parse(text)).toEqual({description: 'handlers </script><b>'});
  });
});
