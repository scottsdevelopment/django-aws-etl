import sys

MIN_PARTS_COUNT = 4
PERCENTAGE_INDEX = 3
GAP_THRESHOLD = 100
MISSING_LINES_START_INDEX = 4

def parse_coverage_output():
    """
    Parses pytest-cov term-missing output from stdin and prints a human-readable summary.
    """
    print("\n🔍 Analyzing Coverage Report...\n")
    
    gaps_found = False
    
    lines = sys.stdin.readlines()
    
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith('-') or line.startswith('=') or "Name" in line and "Stmts" in line:
            continue
            
        parts = line.split()
        if len(parts) < MIN_PARTS_COUNT:
            continue
            
        filename = parts[0]
        if filename.upper() == 'TOTAL':
            continue
            
        try:
            cover_str = parts[PERCENTAGE_INDEX]
            if '%' not in cover_str:
                continue
            
            coverage_pct = int(cover_str.strip('%'))
            
            if coverage_pct < GAP_THRESHOLD:
                gaps_found = True
                if len(parts) > MIN_PARTS_COUNT:
                    missing_lines = " ".join(parts[MISSING_LINES_START_INDEX:])
                else:
                    missing_lines = "Unknown"
                
                print(f"📂 \033[1m{filename}\033[0m")
                print(f"   ❌ Coverage: \033[31m{coverage_pct}%\033[0m")
                print(f"   ⚠️  Missing Lines: {missing_lines}")
                print("-" * 30)
                
        except ValueError:
            continue

    if not gaps_found:
        print("🎉 \033[32mSuccess! 100% Code Coverage Achieved.\033[0m")
    else:
        print("\n\033[33mFix the above gaps to reach 100% coverage.\033[0m")

if __name__ == "__main__":
    parse_coverage_output()
