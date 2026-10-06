import assert from 'node:assert/strict';
import test from 'node:test';
import { reviewPullRequest } from './pr-review.mjs';

const validBody = `## Summary
Fix the behavior.

## Type of change
Bug fix

## How was this tested?
Ran pytest. Refs #42

## Checklist
- [x] Scope is focused and minimal
- [x] Tests and/or checks relevant to this change were executed
- [x] Docs were updated when behavior changed
- [x] No secrets or sensitive data were added
`;

const validPullRequest = {
  title: 'fix: validate job input (#42)',
  body: validBody,
  user: { login: 'contributor' },
};

test('accepts a complete, small, issue-linked pull request', () => {
  const result = reviewPullRequest(validPullRequest, [
    { filename: 'jobs/example.py', status: 'modified', additions: 2, deletions: 1 },
    { filename: 'tests/test_example.py', status: 'modified', additions: 4, deletions: 0 },
  ]);

  assert.deepEqual(result.failures, []);
  assert.deepEqual(result.warnings, []);
  assert.ok(result.messages.includes('Thanks for keeping this pull request small.'));
});

test('requires template sections and issue references and warns on unchecked items', () => {
  const result = reviewPullRequest(
    { title: 'change docs', body: '## Summary\nNo link.', user: { login: 'contributor' } },
    [],
  );

  assert.ok(result.failures.some((failure) => failure.includes('does not reference an issue')));
  assert.ok(result.failures.some((failure) => failure.includes('## Checklist')));
  assert.equal(result.warnings.length, 4);
});

test('exempts release PRs from issue references', () => {
  const result = reviewPullRequest(
    { ...validPullRequest, title: 'chore(release): 1.2.3', body: validBody.replace('Refs #42', '') },
    [],
  );
  assert.ok(!result.failures.some((failure) => failure.includes('does not reference an issue')));
});

test('checks source tests, dependency locks, artifacts, workflow changes, and package manifests', () => {
  const result = reviewPullRequest(validPullRequest, [
    { filename: 'libs/example.py', status: 'modified', additions: 2, deletions: 0 },
    { filename: 'Pipfile', status: 'modified', additions: 1, deletions: 1 },
    { filename: '.github/workflows/check.yml', status: 'modified', additions: 3, deletions: 2 },
    { filename: 'tools/package.json', status: 'added', additions: 10, deletions: 0 },
    { filename: 'dist/archive.zip', status: 'added', additions: 1, deletions: 0 },
  ]);

  assert.ok(result.failures.some((failure) => failure.includes('Build artifacts')));
  assert.ok(result.warnings.some((warning) => warning.includes('without a matching update')));
  assert.ok(result.warnings.some((warning) => warning.includes('Pipfile.lock')));
  assert.ok(result.warnings.some((warning) => warning.includes('package.json')));
  assert.ok(result.messages.some((message) => message.includes('workflows changed')));
});

test('warns when the diff exceeds file or line thresholds', () => {
  const files = Array.from({ length: 11 }, (_, index) => ({
    filename: `docs/file-${index}.md`,
    status: 'modified',
    additions: 20,
    deletions: 0,
  }));
  const result = reviewPullRequest(validPullRequest, files);

  assert.ok(result.warnings.some((warning) => warning.includes('more than 200 lines')));
  assert.ok(result.warnings.some((warning) => warning.includes('more than 10 files')));
});
