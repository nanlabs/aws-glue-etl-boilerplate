import { danger, warn, fail, message, TextDiff } from "danger";

const SMALL_PR_FILES = 10;
const SMALL_PR_LINES = 200;

const DOC_FILE_MATCH = "**/*.md";
// Python source globs by domain
const PY_JOBS_GLOB = "jobs/**/*.py";
const PY_LIBS_GLOB = "libs/**/*.py";
const PY_TESTS_GLOB = "tests/**/*.py";
const WORKFLOWS_GLOB = ".github/workflows/**/*.yml";

const templateSections = [
  "## Description",
  "## Type of Change",
  "## How Has This Been Tested?",
  "## Checklist",
];

const checklistItems = [
  "My code follows the style guidelines of this project",
  "I have performed a self-review of my code",
  "I have commented my code, particularly in hard-to-understand areas",
  "I have made corresponding changes to the documentation",
  "My changes generate no new warnings",
  "Any dependent changes have been merged and published in downstream modules",
  "I have checked my code and corrected any misspellings",
];

const prBody = danger.github.pr.body ?? "";
const releasePrTitle = /^(version packages|chore: version packages|chore\(release\):|chore: release\b)/i;

const hasIssueReference = (text: string) => {
  const cleaned = text
    .replace(/```[\s\S]*?```/g, "")
    .replace(/#ISSUE\b/gi, "");
  const issueReference =
    /\b(?:closes|fixes|resolves|refs|see|related to|part of)\s+(?:#\d+|https:\/\/github\.com\/[\w.-]+\/[\w.-]+\/issues\/\d+)|(?<![#\w])#\d+\b/gim;
  return issueReference.test(cleaned);
};

// No PR is too small to include a description of why you made a change
if (!prBody) {
  const title = ":clipboard: Missing Summary";
  const idea =
    "Can you add a Summary? " +
    "To do so, add a `## Description` section to your PR description. " +
    "This is a good place to explain the motivation for making this change. Include a summary of the changes and the related issue, and list any dependencies that are required for this change.";
  fail(`${title} - <i>${idea}</i>`);
}

if (!danger.github.pr.title) {
  const title = ":id: Missing PR Title";
  const idea = "Can you add the relevant title?";
  fail(`${title} - <i>${idea}</i>`);
}

// Function to check if a section exists in the PR body
const hasSection = (section: string) => prBody.includes(section);

// Function to check if a checklist item is checked in the PR body
const isChecklistItemChecked = (item: string) =>
  prBody.includes(`- [x] ${item}`);

const isIssueReferenceExempt =
  danger.github.pr.user.login.endsWith("[bot]") ||
  danger.github.pr.user.login.startsWith("app/") ||
  releasePrTitle.test(danger.github.pr.title ?? "");

if (
  !isIssueReferenceExempt &&
  !hasIssueReference(prBody) &&
  !hasIssueReference(danger.github.pr.title ?? "")
) {
  fail(
    "This PR does not reference an issue. Please link the related issue with `Closes #N` or `Refs #N` in the PR description. If no issue exists, open one first so maintainers can review the change context."
  );
}

// Check for missing sections
templateSections.forEach((section) => {
  if (!hasSection(section)) {
    fail(
      `:clipboard: Missing Section - Please include the section: <i>${section}</i> in your PR description.`
    );
  }
});

// Check for missing or unchecked checklist items
checklistItems.forEach((item) => {
  if (!isChecklistItemChecked(item)) {
    warn(
      `:clipboard: Unchecked Checklist Item - Please check the item: <i>${item}</i> in your PR description.`
    );
  }
});

const touchedFiles = danger.git.created_files.concat(danger.git.modified_files);
const allFiles = touchedFiles.concat(danger.git.deleted_files);

const diffsList: Promise<(TextDiff | null)[]> = Promise.all(
  allFiles.map((p) => danger.git.diffForFile(p))
);

diffsList
  .then((diffs) => diffs.filter(Boolean) as TextDiff[])
  .then((diffs) => ({
    removed: diffs.reduce(
      (lines, diff) => lines + diff.removed.split("\n").length,
      0
    ),
    added: diffs.reduce(
      (lines, diff) => lines + diff.added.split("\n").length,
      0
    ),
    lines: diffs.reduce(
      (lines, diff) =>
        lines + diff.added.split("\n").length + diff.removed.split("\n").length,
      0
    ),
    files: diffs.length,
  }))
  .then((diff) => {
    if (diff.added < diff.removed) {
      message("Thanks! We :heart: removing more lines than added!");
    }

    if (diff.lines <= SMALL_PR_LINES && diff.files <= SMALL_PR_FILES) {
      message("Thanks! We :heart: small PRs!");
    }

    if (diff.lines > SMALL_PR_LINES) {
      warn(`This PR is changing more than ${SMALL_PR_LINES} lines.`);
    }

    if (diff.files > SMALL_PR_FILES) {
      warn(`This PR is changing more than ${SMALL_PR_FILES} files.`);
    }
  });

// Request changes to src also include changes to tests.
const docs = danger.git.fileMatch(DOC_FILE_MATCH);
const pyJobs = danger.git.fileMatch(PY_JOBS_GLOB);
const pyLibs = danger.git.fileMatch(PY_LIBS_GLOB);
const pyTests = danger.git.fileMatch(PY_TESTS_GLOB);
const workflows = danger.git.fileMatch(WORKFLOWS_GLOB);

if (docs.edited) {
  message("Thanks for updating docs! We :heart: documentation!");
}

if ((pyJobs.created || pyJobs.edited || pyLibs.created || pyLibs.edited) && !(pyTests.created || pyTests.edited)) {
  warn(
    ":test_tube: Source changes detected in jobs/libs without test updates (tests/**/*.py). Consider adding or updating tests."
  );
}

// Warn if Pipfile changed but Pipfile.lock did not (ensure dependency lock is updated)
const pipfile = danger.git.fileMatch("Pipfile");
const pipfileLock = danger.git.fileMatch("Pipfile.lock");
if (
  (pipfile.created ||
    pipfile.modified ||
    pipfile.edited) &&
  !(pipfileLock.created || pipfileLock.modified || pipfileLock.edited)
) {
  warn(
    ":lock: Pipfile changed but Pipfile.lock was not updated. Run 'pipenv lock' to synchronize dependencies."
  );
}

// Prevent committing build artifacts
const artifactAdded = danger.git.created_files.some(
  (p: string) => p.startsWith("build/") || p.startsWith("dist/") || p.endsWith(".zip")
);
if (artifactAdded) {
  fail(
    ":package: Build artifacts detected (build/, dist/, *.zip). Please remove them from the PR; they should be generated in CI only."
  );
}

// Workflows changed: remind linting/docs
if (workflows.edited || workflows.created) {
  message(
    ":gear: GitHub Actions workflows changed. Run actionlint locally and ensure docs/DEPLOYMENT.md reflects any process changes."
  );
}

// Warns if there are changes to any package.json (e.g., tools/danger)
const anyPackageJSON = danger.git.fileMatch("**/package.json");
if (
  anyPackageJSON.modified ||
  anyPackageJSON.created ||
  anyPackageJSON.edited
) {
  warn(
    ":lock: package.json was changed. Ensure lockfiles are updated if applicable."
  );
}
