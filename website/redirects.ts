// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// The AMQP 1.0 guide of the earlier site moved to the crate's rustdoc, whose overview holds it.
const guides: Record<string, string> = {
  amqp: 'index.html',
};

// Pages that moved within the site.
const pages: Record<string, string> = {
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
};

/**
 * Old address to its target, relative to the build's base URL. `docsRs` is the crate's rustdoc
 * for the version built, with a trailing slash.
 */
export function redirects(docsRs: string): Record<string, string> {
  return {
    ...Object.fromEntries(
      Object.entries(guides).map(([from, to]) => [from, `${docsRs}ruststream_amqp/${to}`]),
    ),
    ...pages,
  };
}
