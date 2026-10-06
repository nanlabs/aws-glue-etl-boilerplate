import { appendFileSync, readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const SMALL_PR_FILES = 10;
const SMALL_PR_LINES = 200;

const templateSections = [
  '## Summary',
  '## Type of change',
  '## How was this tested?',
  '## Checklist',
];

const checklistItems = [
  'Scope is focused and minimal',
  'Tests and/or checks relevant to this change were executed',
  'Docs were updated when behavior changed',
  'No secrets or sensitive data were added',
];

const releasePrTitle = /^(version packages|chore: version packages|chore\(release\):|chore: release\b)/i;

function hasIssueReference(text) {
  const cleaned = text.replace(/```[\s\S]*?```/g, '').replace(/#ISSUE\b/gi, '');
  const issueReference =
    /\b(?:closes|fixes|resolves|refs|see|related to|part of)\s+(?:#\d+|https:\/\/github\.com\/[\w.-]+\/[\w.-]+\/issues\/\d+)|(?<![#\w])#\d+\b/gim;
  return issueReference.test(cleaned);
}

function isAddedOrModified(file) {
  return ['added', 'modified', 'renamed', 'copied'].includes(file.status);
}

function matchesAny(files, pattern) {
  return files.some((file) => isAddedOrModified(file) && pattern.test(file.filename));
}

export function reviewPullRequest(pullRequest, files) {
  const failures = [];
  const warnings = [];
  const messages = [];
  const prBody = pullRequest.body ?? '';
  const title = pullRequest.title ?? '';
  const login = pullRequest.user?.login ?? '';

  if (!prBody) {
    failures.push(
      ':clipboard: Missing Summary - Add a `## Summary` section explaining why this change is needed, what changed, related issues, and required dependencies.',
    );
  }

  if (!title) failures.push(':id: Missing PR Title - Add a relevant pull request title.');

  const isIssueReferenceExempt =
    login.endsWith('[bot]') || login.startsWith('app/') || releasePrTitle.test(title);
  if (!isIssueReferenceExempt && !hasIssueReference(prBody) && !hasIssueReference(title)) {
    failures.push(
      'This PR does not reference an issue. Link the related issue with `Closes #N` or `Refs #N` in the description, or open an issue first.',
    );
  }

  for (const section of templateSections) {
    if (!prBody.includes(section)) failures.push(`Missing section: ${section}`);
  }

  for (const item of checklistItems) {
    if (!prBody.includes(`- [x] ${item}`)) warnings.push(`Unchecked checklist item: ${item}`);
  }

  const additions = files.reduce((total, file) => total + (file.additions ?? 0), 0);
  const deletions = files.reduce((total, file) => total + (file.deletions ?? 0), 0);
  const changedLines = additions + deletions;

  if (additions < deletions) messages.push('Thanks for removing more lines than you add.');
  if (changedLines <= SMALL_PR_LINES && files.length <= SMALL_PR_FILES) {
    messages.push('Thanks for keeping this pull request small.');
  }
  if (changedLines > SMALL_PR_LINES) warnings.push(`This PR changes more than ${SMALL_PR_LINES} lines.`);
  if (files.length > SMALL_PR_FILES) warnings.push(`This PR changes more than ${SMALL_PR_FILES} files.`);

  if (matchesAny(files, /(^|\/)jobs\/.*\.py$/) || matchesAny(files, /(^|\/)libs\/.*\.py$/)) {
    if (!matchesAny(files, /(^|\/)tests\/.*\.py$/)) {
      warnings.push('Python source changed under jobs/ or libs/ without a matching update under tests/**/*.py.');
    }
  }

  if (matchesAny(files, /(^|\/).*\.md$/)) messages.push('Thanks for updating documentation.');

  const pipfileChanged = files.some(
    (file) => ['added', 'modified', 'renamed', 'copied'].includes(file.status) && file.filename === 'Pipfile',
  );
  const lockfileChanged = files.some(
    (file) => ['added', 'modified', 'renamed', 'copied'].includes(file.status) && file.filename === 'Pipfile.lock',
  );
  if (pipfileChanged && !lockfileChanged) {
    warnings.push("Pipfile changed but Pipfile.lock did not. Run 'pipenv lock' to synchronize dependencies.");
  }

  if (
    files.some(
      (file) =>
        file.status === 'added' &&
        (file.filename.startsWith('build/') || file.filename.startsWith('dist/') || file.filename.endsWith('.zip')),
    )
  ) {
    failures.push('Build artifacts detected (build/, dist/, *.zip). Remove them; CI should generate these files.');
  }

  if (matchesAny(files, /^\.github\/workflows\/.*\.yml$/)) {
    messages.push('GitHub Actions workflows changed. Run actionlint locally and keep docs/DEPLOYMENT.md in sync.');
  }

  if (matchesAny(files, /(^|\/)package\.json$/)) {
    warnings.push('package.json changed. Ensure applicable lockfiles are updated.');
  }

  return { failures, warnings, messages, additions, deletions, changedLines, changedFiles: files.length };
}

async function fetchChangedFiles(repository, pullNumber, token) {
  const files = [];
  for (let page = 1; ; page += 1) {
    const response = await fetch(
      `https://api.github.com/repos/${repository}/pulls/${pullNumber}/files?per_page=100&page=${page}`,
      {
        headers: {
          Accept: 'application/vnd.github+json',
          Authorization: `Bearer ${token}`,
          'X-GitHub-Api-Version': '2022-11-28',
        },
      },
    );
    if (!response.ok) throw new Error(`GitHub returned HTTP ${response.status} while listing pull request files.`);
    const pageFiles = await response.json();
    files.push(...pageFiles);
    if (pageFiles.length < 100) return files;
  }
}

function renderSummary(review) {
  const sections = [
    '# Pull request review',
    `Files: ${review.changedFiles} · Lines added: ${review.additions} · Lines removed: ${review.deletions}`,
  ];
  if (review.failures.length) sections.push('## Required fixes', ...review.failures.map((item) => `- ${item}`));
  if (review.warnings.length) sections.push('## Warnings', ...review.warnings.map((item) => `- ${item}`));
  if (review.messages.length) sections.push('## Notes', ...review.messages.map((item) => `- ${item}`));
  return `${sections.join('\n')}\n`;
}

async function main() {
  const event = JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8'));
  const pullRequest = event.pull_request;
  if (!pullRequest?.number) throw new Error('This workflow requires a pull_request event.');

  const files = await fetchChangedFiles(
    process.env.GITHUB_REPOSITORY,
    pullRequest.number,
    process.env.GITHUB_TOKEN,
  );
  const review = reviewPullRequest(pullRequest, files);
  const summary = renderSummary(review);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
  process.stdout.write(summary);
  for (const failure of review.failures) process.stdout.write(`::error::${failure}\n`);
  for (const warning of review.warnings) process.stdout.write(`::warning::${warning}\n`);
  for (const message of review.messages) process.stdout.write(`::notice::${message}\n`);
  if (review.failures.length) process.exitCode = 1;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
