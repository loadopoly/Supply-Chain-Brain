// =============================================================================
// CYCLE COUNT ANALYTICS — POWER QUERY M SCRIPTS
// IPS Supply Chain · Astec Industries
//
// HOW TO USE
// ----------
// For each source you want to connect:
//   Power BI Desktop → Transform Data → New Source → Blank Query
//   Advanced Editor → paste the query → Close & Apply
//
// You only need the sources your team actually uses.
// The final UnifiedFactCounts query combines all enabled sources.
// =============================================================================


// ══════════════════════════════════════════════════════════════════════════════
// QUERY 1 — EpicorSource
// Source: Completion_Table export (CSV) from 2026_Cycle_Count_Master.xlsx
//         OR direct from SharePoint / network share
// Output: Unified part-level rows with Q1-Q4 counts, frozen $, abs var $
// ══════════════════════════════════════════════════════════════════════════════
let
    // ── 1a. LOAD ─────────────────────────────────────────────────────────────
    // Option A: Network share CSV  ← uncomment one option, comment others
    RawSource = Csv.Document(
        File.Contents("\\\\server\\IPS_Supply_Chain\\exports\\epicor_completion.csv"),
        [ Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None ]
    ),

    // Option B: SharePoint document library
    // RawSource = Csv.Document(
    //     Web.Contents("https://astecindustries.sharepoint.com/sites/IPS/CC_Dashboard/epicor_completion.csv"),
    //     [ Delimiter = ",", Encoding = 65001 ]
    // ),

    // Option C: Local file (for development / testing)
    // RawSource = Csv.Document(
    //     File.Contents("C:\\Users\\agard\\Downloads\\epicor_completion.csv"),
    //     [ Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None ]
    // ),

    // ── 1b. PROMOTE HEADERS ──────────────────────────────────────────────────
    Promoted   = Table.PromoteHeaders( RawSource, [ PromoteAllScalars = true ] ),

    // ── 1c. SELECT & RENAME COLUMNS ─────────────────────────────────────────
    Selected = Table.SelectColumns( Promoted, {
        "Part", "Part_Description", "Correct_ABC", "WH", "Year",
        "Q1_Count", "Q2_Count", "Q3_Count", "Q4_Count", "Total_Counts",
        "TotFrozenVal", "AbsDollarVar", "ForcedFail", "Overcount_Flag",
        "ClassJump", "FF_ToD"
    }, MissingField.UseNull ),

    Renamed = Table.RenameColumns( Selected, {
        { "Correct_ABC",    "ABC"             },
        { "Part_Description","Desc"           },
        { "Q1_Count",       "Q1_Count"        },
        { "Q2_Count",       "Q2_Count"        },
        { "Q3_Count",       "Q3_Count"        },
        { "Q4_Count",       "Q4_Count"        },
        { "TotFrozenVal",   "Frozen_Dollar"   },
        { "AbsDollarVar",   "Abs_Dollar_Var"  }
    }),

    // ── 1d. SET TYPES ─────────────────────────────────────────────────────────
    Typed = Table.TransformColumnTypes( Renamed, {
        { "Part",           type text    },
        { "Desc",           type text    },
        { "ABC",            type text    },
        { "WH",             type text    },
        { "Year",           Int64.Type   },
        { "Q1_Count",       Int64.Type   },
        { "Q2_Count",       Int64.Type   },
        { "Q3_Count",       Int64.Type   },
        { "Q4_Count",       Int64.Type   },
        { "Total_Counts",   Int64.Type   },
        { "Frozen_Dollar",  type number  },
        { "Abs_Dollar_Var", type number  },
        { "ForcedFail",     type text    },
        { "Overcount_Flag", type text    }
    }),

    // ── 1e. CLEAN & STANDARDIZE ───────────────────────────────────────────────
    Cleaned = Table.TransformColumns( Typed, {
        { "ABC",            Text.Upper },
        { "WH",             Text.Upper },
        { "ForcedFail",     Text.Upper },
        { "Overcount_Flag", Text.Upper }
    }),

    // Replace nulls in numeric columns with 0
    FilledNulls = Table.ReplaceValue(
        Table.ReplaceValue(
            Table.ReplaceValue(
                Table.ReplaceValue( Cleaned, null, 0, Replacer.ReplaceValue, {"Q1_Count","Q2_Count","Q3_Count","Q4_Count"} ),
                null, 0.0, Replacer.ReplaceValue, {"Frozen_Dollar"} ),
            null, 0.0, Replacer.ReplaceValue, {"Abs_Dollar_Var"} ),
        null, "N", Replacer.ReplaceValue, {"ForcedFail"} ),

    // ── 1f. ADD METADATA COLUMNS ─────────────────────────────────────────────
    AddSource      = Table.AddColumn( FilledNulls, "Source",      each "EPICOR",              type text      ),
    AddRefreshedAt = Table.AddColumn( AddSource,   "RefreshedAt", each DateTime.LocalNow(),   type datetime  ),

    // ── 1g. FILTER OUT EXCLUSION CLASSES (optional — remove if D class needed) ─
    // FilteredABCD = Table.SelectRows( AddRefreshedAt, each [ABC] <> "X" ),

    Output = AddRefreshedAt
in
    Output


// ══════════════════════════════════════════════════════════════════════════════
// QUERY 2 — OracleSource
// Source: PASTE_RawCounts sheet (CSV) from Oracle_Count_Completion_rev15.xlsx
// Output: One row per Item, with Q1-Q4 aggregated from Count Date
// ══════════════════════════════════════════════════════════════════════════════
let
    // ── 2a. LOAD ─────────────────────────────────────────────────────────────
    RawSource = Csv.Document(
        File.Contents("\\\\server\\IPS_Supply_Chain\\exports\\oracle_rawcounts.csv"),
        [ Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None ]
    ),

    // Option B: SharePoint
    // RawSource = Csv.Document(
    //     Web.Contents("https://astecindustries.sharepoint.com/sites/IPS/CC_Dashboard/oracle_rawcounts.csv"),
    //     [ Delimiter = ",", Encoding = 65001 ]
    // ),

    // ── 2b. PROMOTE HEADERS ──────────────────────────────────────────────────
    Promoted = Table.PromoteHeaders( RawSource, [ PromoteAllScalars = true ] ),

    // ── 2c. SELECT RELEVANT COLUMNS ──────────────────────────────────────────
    Selected = Table.SelectColumns( Promoted, {
        "Item", "Desc", "ABC", "Sub-inventory", "Count Date",
        "Location Dollars", "Absolute Dollars Adjusted",
        "Count Sequence Status", "Recounted"
    }, MissingField.UseNull ),

    // ── 2d. TYPE DATE & NUMBERS ───────────────────────────────────────────────
    Typed = Table.TransformColumnTypes( Selected, {
        { "Item",                         type text    },
        { "Desc",                         type text    },
        { "ABC",                          type text    },
        { "Sub-inventory",                type text    },
        { "Count Date",                   type date    },
        { "Location Dollars",             type number  },
        { "Absolute Dollars Adjusted",    type number  }
    }),

    // ── 2e. DERIVE QUARTER ────────────────────────────────────────────────────
    AddQtr = Table.AddColumn( Typed, "Qtr",
        each Date.QuarterOfYear( [Count Date] ), Int64.Type ),

    // ── 2f. AGGREGATE TO PART LEVEL ───────────────────────────────────────────
    Grouped = Table.Group( AddQtr, {"Item","Desc","ABC","Sub-inventory"}, {
        { "Q1_Count",      each Table.RowCount( Table.SelectRows( _, each [Qtr] = 1 ) ), Int64.Type  },
        { "Q2_Count",      each Table.RowCount( Table.SelectRows( _, each [Qtr] = 2 ) ), Int64.Type  },
        { "Q3_Count",      each Table.RowCount( Table.SelectRows( _, each [Qtr] = 3 ) ), Int64.Type  },
        { "Q4_Count",      each Table.RowCount( Table.SelectRows( _, each [Qtr] = 4 ) ), Int64.Type  },
        { "Total_Counts",  each Table.RowCount( _ ),                                      Int64.Type  },
        { "Frozen_Dollar", each List.Sum( _[Location Dollars] ),                          type number },
        { "Abs_Dollar_Var",each List.Sum( _[Absolute Dollars Adjusted] ),                 type number }
    }),

    // ── 2g. RENAME & ADD COLUMNS ─────────────────────────────────────────────
    Renamed = Table.RenameColumns( Grouped, {
        { "Sub-inventory", "WH" }
    }),

    AddForcedFail  = Table.AddColumn( Renamed, "ForcedFail",     each "N",                  type text     ),
    AddOvercount   = Table.AddColumn( AddForcedFail, "Overcount_Flag", each "NO",            type text     ),
    AddYear        = Table.AddColumn( AddOvercount,  "Year",     each 2026,                  Int64.Type    ),
    AddSource      = Table.AddColumn( AddYear,       "Source",   each "ORACLE",              type text     ),
    AddRefreshedAt = Table.AddColumn( AddSource,     "RefreshedAt", each DateTime.LocalNow(),type datetime ),

    Output = AddRefreshedAt
in
    Output


// ══════════════════════════════════════════════════════════════════════════════
// QUERY 3 — SytelineSource
// Source: DS_VarianceSummary export (CSV) from SSRS
//         OR DS_CountCompletion CSV
// Output: Summary-level rows (one per ABC class unless using part-level DS)
// ══════════════════════════════════════════════════════════════════════════════
let
    RawSource = Csv.Document(
        File.Contents("\\\\server\\IPS_Supply_Chain\\exports\\syteline_summary.csv"),
        [ Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.None ]
    ),

    Promoted = Table.PromoteHeaders( RawSource, [ PromoteAllScalars = true ] ),

    // DS_VarianceSummary columns: ABC_Class, Count_Sequences, Total_Pre_Dollar,
    // Total_Post_Dollar, Sum_Abs_Var_Dollar, Value_Accuracy, Total_Pre_QOH,
    // Sum_Abs_Var_QOH, QOH_Accuracy, Fulfilled, Remaining, Completion_Pct, Frequency

    Selected = Table.SelectColumns( Promoted, {
        "ABC_Class", "Fulfilled", "Count_Sequences",
        "Total_Pre_Dollar", "Sum_Abs_Var_Dollar", "Value_Accuracy",
        "Completion_Pct", "Frequency"
    }, MissingField.UseNull ),

    Typed = Table.TransformColumnTypes( Selected, {
        { "ABC_Class",          type text   },
        { "Fulfilled",          Int64.Type  },
        { "Count_Sequences",    Int64.Type  },
        { "Total_Pre_Dollar",   type number },
        { "Sum_Abs_Var_Dollar",  type number },
        { "Value_Accuracy",     type number },
        { "Completion_Pct",     type number }
    }),

    // Expand to part-level pseudo rows so it merges with the unified model.
    // NOTE: If you have DS_CountCompletion (part-level), use that directly
    // instead — it has Part, ABC, Q1_Count … Q4_Count columns matching the
    // Epicor format. Skip the expansion step below and use the same pattern
    // as EpicorSource.

    // For summary-level SSRS output, we create one synthetic row per ABC class.
    // These show correct variance totals in the Variance page.
    Expanded = Table.AddColumn( Typed, "Part",
        each [ABC_Class] & "-SSRS-SUMMARY", type text ),
    AddDesc    = Table.AddColumn( Expanded,  "Desc",          each "SSRS Summary",    type text     ),
    AddABC     = Table.AddColumn( AddDesc,   "ABC",           each [ABC_Class],       type text     ),
    AddWH      = Table.AddColumn( AddABC,    "WH",            each "ALL",             type text     ),
    AddQ1      = Table.AddColumn( AddWH,     "Q1_Count",      each 0,                 Int64.Type    ),
    AddQ2      = Table.AddColumn( AddQ1,     "Q2_Count",      each 0,                 Int64.Type    ),
    AddQ3      = Table.AddColumn( AddQ2,     "Q3_Count",      each 0,                 Int64.Type    ),
    AddQ4      = Table.AddColumn( AddQ3,     "Q4_Count",      each if [Fulfilled] > 0 then 1 else 0, Int64.Type ),
    AddTotal   = Table.AddColumn( AddQ4,     "Total_Counts",  each [Fulfilled],       Int64.Type    ),
    AddFrozen  = Table.AddColumn( AddTotal,  "Frozen_Dollar", each [Total_Pre_Dollar],type number   ),
    AddAbs     = Table.AddColumn( AddFrozen, "Abs_Dollar_Var",each [Sum_Abs_Var_Dollar],type number ),
    AddFF      = Table.AddColumn( AddAbs,    "ForcedFail",    each "N",              type text     ),
    AddOC      = Table.AddColumn( AddFF,     "Overcount_Flag",each "NO",             type text     ),
    AddYear    = Table.AddColumn( AddOC,     "Year",          each 2026,             Int64.Type    ),
    AddSource  = Table.AddColumn( AddYear,   "Source",        each "SYTELINE",       type text     ),
    AddRefresh = Table.AddColumn( AddSource, "RefreshedAt",   each DateTime.LocalNow(), type datetime ),

    // Keep only unified columns
    Output = Table.SelectColumns( AddRefresh, {
        "Part","Desc","ABC","WH","Year",
        "Q1_Count","Q2_Count","Q3_Count","Q4_Count","Total_Counts",
        "Frozen_Dollar","Abs_Dollar_Var","ForcedFail","Overcount_Flag",
        "Source","RefreshedAt"
    })
in
    Output


// ══════════════════════════════════════════════════════════════════════════════
// QUERY 4 — FactCounts  (MAIN TABLE — references the 3 sources above)
// This is the table all your DAX calculated columns and measures use.
// ══════════════════════════════════════════════════════════════════════════════
let
    // Pull all enabled sources — comment out any you're not using
    Epicor    = EpicorSource,
    Oracle    = OracleSource,
    Syteline  = SytelineSource,

    // Combine into single table
    Combined  = Table.Combine( { Epicor, Oracle, Syteline } ),

    // Remove rows where ABC is blank, null, or "X" (excluded)
    Filtered  = Table.SelectRows( Combined,
        each [ABC] <> null and [ABC] <> "" and [ABC] <> "X" ),

    // Ensure ABC is trimmed and uppercase
    CleanABC  = Table.TransformColumns( Filtered, {
        { "ABC", each Text.Upper( Text.Trim( _ ) ), type text }
    }),

    // Add a unique row key (useful for direct query / incremental refresh)
    AddKey    = Table.AddIndexColumn( CleanABC, "RowKey", 1, 1, Int64.Type ),

    Output    = AddKey
in
    Output


// ══════════════════════════════════════════════════════════════════════════════
// QUERY 5 — FactVarianceDetail  (OPTIONAL — for drill-through variance detail)
// Source: Variance_Table from Epicor dashboard
//         or Count_Groups from Oracle
// ══════════════════════════════════════════════════════════════════════════════
let
    // Load Epicor Variance_Table (if available as CSV)
    RawSource = Csv.Document(
        File.Contents("\\\\server\\IPS_Supply_Chain\\exports\\epicor_variance_detail.csv"),
        [ Delimiter = ",", Encoding = 65001 ]
    ),
    Promoted  = Table.PromoteHeaders( RawSource, [ PromoteAllScalars = true ] ),
    Selected  = Table.SelectColumns( Promoted, {
        "Group_ID","Part","Correct_ABC","Qtr","WH",
        "Net_Dollar_Var","Abs_Dollar_Var","Has_Variance","Count_Status","Year"
    }, MissingField.UseNull ),
    Typed = Table.TransformColumnTypes( Selected, {
        { "Group_ID",       type text   },
        { "Part",           type text   },
        { "Correct_ABC",    type text   },
        { "Qtr",            Int64.Type  },
        { "WH",             type text   },
        { "Net_Dollar_Var", type number },
        { "Abs_Dollar_Var", type number },
        { "Has_Variance",   type text   },
        { "Count_Status",   type text   },
        { "Year",           Int64.Type  }
    }),
    AddSource = Table.AddColumn( Typed, "Source", each "EPICOR", type text ),
    Output    = AddSource
in
    Output
