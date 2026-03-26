SCREENER_TOP_N      <- 10
SCREENER_LOOKBACK   <- 180
SCREENER_USE_CACHE  <- TRUE
ENGINE_MARKET_INDEX  <- "^GSPC"
ENGINE_SECTOR_INDEX  <- "XLV"
ENGINE_VIX_INDEX     <- "^VIX"
ENGINE_TREASURY_10Y  <- "^TNX"
ENGINE_TRAIN_WINDOW  <- 252 * 10
ENGINE_N_FOLDS       <- 6
ENGINE_FORECAST_DAYS <- 20
ENGINE_NEUTRAL_BAND  <- 0.015
SCREENER_BLACKLIST  <- c()
SCREENER_WHITELIST  <- NULL
SCRIPT_DIR  <- dirname(rstudioapi::getActiveDocumentContext()$path)
OUTPUT_DIR  <- file.path(SCRIPT_DIR, "quant_output")
dir.create(OUTPUT_DIR, showWarnings = FALSE)
cat(paste0("\n", strrep("=", 80), "\n"))
cat("   QUANT ENGINE RUNNER v1.0\n")
cat(paste0(strrep("=", 80), "\n"))
screener_path <- file.path(SCRIPT_DIR, "ALPHA_SCREENER.R")
if (!file.exists(screener_path)) stop("ALPHA_SCREENER.R nu a fost gasit in: ", SCRIPT_DIR)
source(screener_path, local = FALSE)
screener_result <- run_alpha_screener(
  top_n         = SCREENER_TOP_N,
  lookback_days = SCREENER_LOOKBACK,
  blacklist     = SCREENER_BLACKLIST,
  whitelist     = SCREENER_WHITELIST,
  use_cache     = SCREENER_USE_CACHE
)
top_tickers   <- screener_result$top_tickers
screener_dt   <- screener_result$screener_dt
cat(sprintf("\n[RUNNER] Tickers selectati de screener: %s\n",
            paste(top_tickers, collapse = ", ")))
engine_path <- file.path(SCRIPT_DIR, "INSTITUTIONAL QUANT ENGINE V3.R")
if (!file.exists(engine_path)) stop("Engine-ul nu a fost gasit in: ", SCRIPT_DIR)
all_signals  <- list()
for (ticker in top_tickers) {
  cat(paste0("\n", strrep("-", 80), "\n"))
  cat(sprintf("   RULARE ENGINE: %s (%d/%d)\n",
              ticker,
              which(top_tickers == ticker),
              length(top_tickers)))
  cat(paste0(strrep("-", 80), "\n"))
  SYMBOL       <<- ticker
  MARKET_INDEX <<- ENGINE_MARKET_INDEX
  SECTOR_INDEX <<- ENGINE_SECTOR_INDEX
  VIX_INDEX    <<- ENGINE_VIX_INDEX
  TREASURY_10Y <<- ENGINE_TREASURY_10Y
  TRAIN_WINDOW <<- ENGINE_TRAIN_WINDOW
  N_FOLDS      <<- ENGINE_N_FOLDS
  FORECAST_DAYS <<- ENGINE_FORECAST_DAYS
  NEUTRAL_BAND <<- ENGINE_NEUTRAL_BAND
  OUTPUT_DIR   <<- file.path(SCRIPT_DIR, "quant_output", ticker)
  dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
  result <- tryCatch({
    source(engine_path, local = FALSE)
    list(
      ticker          = ticker,
      signal          = if (exists("latest_pred_xgb")) latest_pred_xgb else NA,
      current_price   = if (exists("current_price"))   current_price   else NA,
      target_price    = if (exists("current_price") && exists("latest_pred_xgb"))
                          current_price * (1 + latest_pred_xgb) else NA,
      vol_regime      = if (exists("current_vol_regime")) current_vol_regime else NA,
      mkt_regime      = if (exists("current_mkt_regime")) current_mkt_regime else NA,
      vix_corr        = if (exists("current_vix_corr")) current_vix_corr else NA,
      sharpe          = if (exists("metrics")) metrics$sharpe_ratio else NA,
      total_return    = if (exists("metrics")) metrics$total_return  else NA,
      alpha_score     = screener_dt[Ticker == ticker, Alpha_Score],
      screener_roc60  = screener_dt[Ticker == ticker, ROC_60d],
      status          = "OK"
    )
  }, error = function(e) {
    cat(sprintf("\n[RUNNER] EROARE pentru %s: %s\n", ticker, conditionMessage(e)))
    list(
      ticker          = ticker,
      signal          = NA,
      current_price   = NA,
      target_price    = NA,
      vol_regime      = NA,
      mkt_regime      = NA,
      vix_corr        = NA,
      sharpe          = NA,
      total_return    = NA,
      alpha_score     = screener_dt[Ticker == ticker, Alpha_Score],
      screener_roc60  = screener_dt[Ticker == ticker, ROC_60d],
      status          = paste0("EROARE: ", conditionMessage(e))
    )
  })
  all_signals[[ticker]] <- result
  cat(sprintf("\n[RUNNER] %s completat. Semnal: %+.2f%%\n",
              ticker,
              if (!is.na(result$signal)) result$signal * 100 else 0))
}
cat(paste0("\n", strrep("=", 80), "\n"))
cat("   RAPORT COMPARATIV  ALPHA SCREENER + QUANT ENGINE\n")
cat(sprintf("   Data: %s | Orizont: %d zile\n", Sys.Date(), ENGINE_FORECAST_DAYS))
cat(paste0(strrep("=", 80), "\n"))
cat("\n     AVERTISMENT INSTITUTIONAL (LOOK-AHEAD BIAS):\n")
cat("   - Nu folosi valoarea 'Total_Ret_Pct' sau 'Sharpe' pentru a valida strategia \n")
cat("     pe ultimii 10 ani. Aceste metrici ale backtest-ului sunt corupte/fals-pozitive,\n")
cat("     deoarece Screener-ul a ales actiunile uitandu-se la performanta lor de azi \n")
cat("     (stiind deja viitorul lor). Aceasta se numeste Dynamic Universe Selection Leakage.\n")
cat("   - SCOP REAL: Foloseste exclusiv coloana 'Pred_Ret_Pct' (Semnalul Live) si \n")
cat("     'Target_Price'. Aceastea sunt corecte matematic si iti ofera decizia reala \n")
cat("     pentru ziua de MAINE, folosind un model antrenat doar pe trecut.\n\n")
summary_rows <- lapply(all_signals, function(r) {
  data.table(
    Ticker        = r$ticker,
    Status        = r$status,
    Price         = round(r$current_price, 2),
    Pred_Ret_Pct  = round(r$signal * 100, 2),
    Target_Price  = round(r$target_price, 2),
    Vol_Regime    = r$vol_regime,
    Sharpe        = round(r$sharpe, 2),
    Total_Ret_Pct = round(r$total_return * 100, 2),
    Alpha_Score   = round(r$alpha_score, 3),
    ROC_60d_Pct   = round(r$screener_roc60 * 100, 2)
  )
})
summary_dt <- rbindlist(summary_rows, fill = TRUE)
setorder(summary_dt, -Pred_Ret_Pct)
cat("SELECTIE FINALA (sortata dupa return prezis):\n")
print(summary_dt)
best_ticker <- summary_dt$Ticker[1]
best_signal <- summary_dt$Pred_Ret_Pct[1]
cat(sprintf("\n[BEST PICK] %s | Pred: %+.2f%% | Target: $%.2f\n",
            best_ticker,
            best_signal,
            summary_dt$Target_Price[1]))
report_file <- file.path(SCRIPT_DIR, "quant_output",
                         sprintf("runner_report_%s.csv", Sys.Date()))
fwrite(summary_dt, report_file)
cat(sprintf("\n[OK] Raport salvat in: %s\n", report_file))
cat(paste0("\n", strrep("=", 80), "\n"))
cat("   RUNNER FINALIZAT\n")
cat(paste0(strrep("=", 80), "\n\n"))
cat("   Generare Dashboard Vizual...\n")
if (!require("ggplot2")) install.packages("ggplot2")
library(ggplot2)
p_dash <- ggplot(summary_dt, aes(x = reorder(Ticker, Pred_Ret_Pct), y = Pred_Ret_Pct, fill = Pred_Ret_Pct > 0)) +
  geom_bar(stat = "identity", color = "black", alpha = 0.8, width = 0.7) +
  coord_flip() +
  scale_fill_manual(values = c("TRUE" = "#2ecc71", "FALSE" = "#e74c3c")) +
  geom_text(aes(label = sprintf("%+.2f%%", Pred_Ret_Pct)),
            hjust = ifelse(summary_dt$Pred_Ret_Pct > 0, -0.2, 1.2),
            size = 5, fontface = "bold", color = "black") +
  labs(title = " Quant Engine: Previziuni Top Tickers (20 Zile)",
       subtitle = sprintf("Generat pe: %s | Model: XGBoost + LightGBM", Sys.Date()),
       x = "Ticker (Simbol)",
       y = "Randament Previzionat (%)") +
  theme_minimal(base_size = 14) +
  theme(legend.position = "none",
        plot.title = element_text(face = "bold", color = "#2c3e50"),
        axis.text.y = element_text(face = "bold", size = 12))
print(p_dash)
dash_file <- file.path(OUTPUT_DIR, sprintf("dashboard_vizual_%s.png", Sys.Date()))
ggsave(dash_file, plot = p_dash, width = 10, height = 6, dpi = 300, bg = "white")
cat(sprintf("[OK] Dashboard vizual salvat in: %s\n", dash_file))
cat(paste0(strrep("=", 80), "\n\n"))
