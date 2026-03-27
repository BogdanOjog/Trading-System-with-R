if (!require("pacman")) install.packages("pacman")
pacman::p_load(
  quantmod, data.table, rvest, httr,
  TTR,
  dplyr
)
SCREENER_CACHE_DIR <- "screener_cache" 
dir.create(SCREENER_CACHE_DIR, showWarnings = FALSE)
get_sp500_universe <- function(use_cache = TRUE, max_age_days = 30) {
  cache_file <- file.path(SCREENER_CACHE_DIR, "sp500_universe_static.rds")
  if (use_cache && file.exists(cache_file)) {
    file_info <- file.info(cache_file)
    file_age <- as.numeric(difftime(Sys.time(), file_info$mtime, units = "days"))
    if (file_age <= max_age_days) {
      cat(sprintf("   [cache] Universul S&P 500 citit instantaneu de pe disc (vechime: %.1f zile).\n", file_age))
      return(readRDS(cache_file))
    } else {
      cat("   [cache] Lista S&P 500 a expirat (mai veche de 30 zile). Actualizam...\n")
    }
  } else {
    cat("   [cache] Nu s-a gasit lista locala. Descarcam de pe Wikipedia...\n")
  }
  tryCatch({
    url  <- "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    page <- rvest::read_html(url)
    tbl  <- rvest::html_table(page, fill = TRUE)[[1]]
    tickers <- as.character(tbl$Symbol)
    tickers <- gsub("\\.", "-", tickers)
    tickers <- tickers[nchar(tickers) > 0 & !is.na(tickers)]
    cat(sprintf("   [OK] %d tickers descarcati si salvati in cache pentru %d zile.\n", length(tickers), max_age_days))
    if (use_cache) saveRDS(tickers, cache_file)
    return(tickers)
  }, error = function(e) {
    cat(sprintf("   [WARN] Wikipedia indisponibil: %s\n", conditionMessage(e)))
    if (file.exists(cache_file)) {
        cat("   [FALLBACK] Folosim cache-ul expirat din lipsa de conexiune.\n")
        return(readRDS(cache_file))
    }
    cat("   [FALLBACK CRITIC] Folosim lista hardcodata de 100 tickers...\n")
    fallback <- c(
      "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","BRK-B","LLY","AVGO",
      "JPM","TSLA","V","UNH","XOM","MA","PG","COST","JNJ","HD",
      "ABBV","MRK","CVX","NFLX","CRM","AMD","WMT","BAC","ACN","PEP",
      "KO","TMO","CSCO","ABT","MCD","ADBE","TXN","DHR","NEE","PM",
      "AMGN","GE","QCOM","HON","LOW","CAT","IBM","GS","SPGI","AXP",
      "INTU","AMAT","ISRG","RTX","PLD","BLK","MDLZ","ADI","SYK","VRTX",
      "GILD","TJX","REGN","MMC","ETN","CB","ADP","SCHW","CME","DE",
      "AMT","EOG","MO","BSX","PGR","SHW","LRCX","KLAC","SO","DUK",
      "ICE","ZTS","MCO","WM","MMM","ITW","CL","USB","PNC","FDX",
      "NSC","EMR","AON","HCA","TDG","ORLY","MCHP","PSA","O","D"
    )
    return(fallback)
  })
}
screen_single_ticker <- function(ticker, lookback_days = 180) {
  tryCatch({
    start_date <- Sys.Date() - lookback_days - 30
    raw <- tryCatch(
      getSymbols(ticker, src = "yahoo", from = start_date, auto.assign = FALSE,
                 warnings = FALSE),
      error = function(e) NULL
    )
    if (is.null(raw) || nrow(raw) < 60) return(NULL)
    dt <- data.table(
      Date  = index(raw),
      Close = as.numeric(Cl(raw)),
      High  = as.numeric(Hi(raw)),
      Low   = as.numeric(Lo(raw)),
      Vol   = as.numeric(Vo(raw))
    )
    dt <- dt[!is.na(Close) & Close > 0]
    if (nrow(dt) < 60) return(NULL)
    n <- nrow(dt)
    roc_60 <- (dt$Close[n] / dt$Close[max(1, n - 60)]) - 1
    sma200_val <- if (n >= 200) mean(dt$Close[(n-199):n]) else mean(dt$Close)
    sma200_dist <- (dt$Close[n] / sma200_val) - 1
    recent <- tail(dt, 14)
    atr_abs <- mean(recent$High - recent$Low, na.rm = TRUE)
    atr_pct <- atr_abs / dt$Close[n]
    daily_rets <- diff(log(dt$Close))
    vol_60d <- sd(tail(daily_rets, 60), na.rm = TRUE) * sqrt(252)
    risk_adj_mom <- roc_60 / max(vol_60d, 0.001)
    data.table(
      Ticker       = ticker,
      Price        = round(dt$Close[n], 2),
      ROC_60d      = round(roc_60, 4),
      SMA200_Dist  = round(sma200_dist, 4),
      ATR_Pct      = round(atr_pct, 4),
      Vol_60d      = round(vol_60d, 4),
      Risk_Adj_Mom = round(risk_adj_mom, 4)
    )
  }, error = function(e) NULL)
}
screen_universe <- function(tickers,
                            top_n         = 10,
                            lookback_days = 180,
                            pit_date      = NULL,
                            sleep_sec     = 0.5,
                            use_cache     = TRUE) {
  date_key   <- if (!is.null(pit_date)) as.character(pit_date) else as.character(Sys.Date())
  cache_file <- file.path(SCREENER_CACHE_DIR,
                          sprintf("screen_results_%s_top%d.rds", date_key, top_n))
  if (use_cache && file.exists(cache_file)) {
    cat("   [cache] Rezultate screener incarcate din cache.\n")
    return(readRDS(cache_file))
  }
  cat(sprintf("   Screening %d tickers (lookback: %d zile)...\n",
              length(tickers), lookback_days))
  cat("   Timp estimat prima rulare: ~", round(length(tickers) * sleep_sec / 60, 1), "minute\n")
  results <- list()
  n_ok <- 0
  n_fail <- 0
  for (i in seq_along(tickers)) {
    ticker <- tickers[i]
    if (i %% 50 == 0) {
      cat(sprintf("   Progress: %d/%d (OK: %d | Fail: %d)\n",
                  i, length(tickers), n_ok, n_fail))
    }
    row <- screen_single_ticker(ticker, lookback_days = lookback_days)
    if (!is.null(row)) {
      results[[length(results) + 1]] <- row
      n_ok <- n_ok + 1
    } else {
      n_fail <- n_fail + 1
    }
    Sys.sleep(sleep_sec)
  }
  if (length(results) == 0) {
    stop("Screener: niciun ticker valid gasit!")
  }
  dt_all <- rbindlist(results)
  zscore <- function(x) {
    s <- sd(x, na.rm = TRUE)
    if (is.na(s) || s == 0) return(rep(0, length(x)))
    (x - mean(x, na.rm = TRUE)) / s
  }
  dt_all[, z_Mom      := zscore(ROC_60d)]
  dt_all[, z_RiskAdj  := zscore(Risk_Adj_Mom)]
  dt_all[, z_SMA200   := zscore(SMA200_Dist)]
  dt_all[, z_VolPen   := zscore(ATR_Pct)]
  dt_all[, Alpha_Score := 0.15 * z_Mom +
                           0.40 * z_RiskAdj +
                           0.25 * z_SMA200 -
                           0.30 * z_VolPen]
  dt_filtered <- dt_all[SMA200_Dist > 0]
  if (nrow(dt_filtered) == 0) {
    cat("   [WARN] Niciun ticker nu trece filtrul SMA200 > 0 (bear market?). Folosim top fara filtru.\n")
    dt_filtered <- dt_all
  }
  setorder(dt_filtered, -Alpha_Score)
  top_result <- head(dt_filtered, top_n)
  cat(sprintf("\n   [SCREENER] TOP %d SELECTATE:\n", top_n))
  print(top_result[, .(Ticker, Price, ROC_60d, SMA200_Dist, ATR_Pct, Alpha_Score)])
  if (use_cache) saveRDS(top_result, cache_file)
  return(top_result)
}
run_alpha_screener <- function(top_n         = 5,
                               lookback_days = 180,
                               use_cache     = TRUE,
                               blacklist     = c(),
                               whitelist     = NULL,
                               pit_date      = NULL) {
  cat(paste0("\n", strrep("=", 80), "\n"))
  cat("   SCREENER_SCRIPT\n")
  cat(paste0(strrep("=", 80), "\n"))
  universe <- if (!is.null(whitelist)) {
    cat(sprintf("   [WHITELIST] Folosind lista personalizata de %d tickers.\n", length(whitelist)))
    whitelist
  } else {
    get_sp500_universe(use_cache = use_cache)
  }
  if (length(blacklist) > 0) {
    universe <- setdiff(universe, blacklist)
    cat(sprintf("   [BLACKLIST] %d tickers exclusi.\n", length(blacklist)))
  }
  screen_dt <- screen_universe(
    tickers       = universe,
    top_n         = top_n,
    lookback_days = lookback_days,
    pit_date      = pit_date,
    use_cache     = use_cache
  )
  top_tickers <- screen_dt$Ticker
  cat(sprintf("\n   Tickers selectati: %s\n", paste(top_tickers, collapse = ", ")))
  cat(paste0(strrep("=", 80), "\n\n"))
  return(list(
    top_tickers = top_tickers,
    screener_dt = screen_dt,
    metadata    = list(
      run_date      = Sys.Date(),
      pit_date      = pit_date,
      top_n         = top_n,
      lookback_days = lookback_days,
      universe_size = length(universe)
    )
  ))
}
if (sys.nframe() == 0) {
  TOP_N         <- 10
  LOOKBACK_DAYS <- 180
  BLACKLIST     <- c()
  WHITELIST     <- NULL
  screener_result <- run_alpha_screener(
    top_n         = TOP_N,
    lookback_days = LOOKBACK_DAYS,
    blacklist     = BLACKLIST,
    whitelist     = WHITELIST,
    use_cache     = TRUE
  )
  cat("Top tickers selectate pentru ENGINE_SCRIPT:\n")
  print(screener_result$screener_dt)
}
