echo ""
echo "Security scan with TruffleHog ..."
trufflehog git file://$PWD --filter-entropy=3.0 --json --log-level=-1 \
| jq -cs '{findings: length, ok: (length==0), results: .}' > trufflehog_scan_report.json
jq . trufflehog_scan_report.json
rm trufflehog_scan_report.json

echo ""
echo "Security scan with Gitleaks ..."
gitleaks detect --source=. --config .gitleaks.toml --redact --report-format json --report-path gitleaks_scan_report.json --exit-code 0
jq . gitleaks_scan_report.json
rm gitleaks_scan_report.json

echo ""
echo "Security scan completed."
echo ""

