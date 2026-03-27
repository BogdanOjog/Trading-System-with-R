if (!require("pacman")) install.packages("pacman")
pacman::p_load(quantmod, data.table, xgboost, lightgbm,
               Rcpp, RcppParallel, foreach, doParallel, caret,
               ggplot2, scales, PerformanceAnalytics, zoo, TTR,
               quantreg, kernlab)
USE_GPU <- FALSE
cat("  GPU Acceleration Disabled (Running in CPU Compatibility Mode)\n")
if (!exists("SYMBOL"))        SYMBOL        <- "ABT"
if (!exists("MARKET_INDEX"))  MARKET_INDEX  <- "^GSPC"
if (!exists("SECTOR_INDEX"))  SECTOR_INDEX  <- "XLV"
if (!exists("VIX_INDEX"))     VIX_INDEX     <- "^VIX"
if (!exists("TREASURY_10Y"))  TREASURY_10Y  <- "^TNX"
if (!exists("FORECAST_DAYS")) FORECAST_DAYS <- 20
if (!exists("TRAIN_WINDOW"))  TRAIN_WINDOW  <- 252 * 7
if (!exists("N_FOLDS"))       N_FOLDS       <- 12
if (!exists("NEUTRAL_BAND"))  NEUTRAL_BAND  <- 0.015
cat(paste0("\n", strrep("=", 100)))
cat(paste0("\n  INITIALIZING QUANT ENGINE v3.1 (NO PYTHON DEPENDENCY): ", SYMBOL))
cat(paste0("\n", strrep("=", 100), "\n"))
Rcpp::sourceCpp(code = '
#include <Rcpp.h>
#include <cmath>
#include <vector>
#include <algorithm>
#include <numeric>
using namespace Rcpp;
// [[Rcpp::export]]
NumericVector cpp_obv_fast(const NumericVector& close,
                          const NumericVector& volume) {
  int n = close.size();
  NumericVector obv(n);
  double cum_obv = 0.0;
  obv[0] = 0.0;
  for(int i = 1; i < n; ++i) {
    if(close[i] > close[i-1]) {
      cum_obv += volume[i];
    } else if(close[i] < close[i-1]) {
      cum_obv -= volume[i];
    }
    obv[i] = cum_obv;
  }
  return obv;
}
// [[Rcpp::export]]
NumericVector cpp_rolling_corr(const NumericVector& x,
                              const NumericVector& y,
                              int window) {
  int n = x.size();
  NumericVector result(n, NA_REAL);
  if(n != y.size() || window > n || window < 2) {
    return result;
  }
  for(int i = window - 1; i < n; ++i) {
    double sum_x = 0.0, sum_y = 0.0, sum_xy = 0.0;
    double sum_x2 = 0.0, sum_y2 = 0.0;
    int count = 0;
    for(int j = 0; j < window; ++j) {
      int idx = i - window + 1 + j;
      if(!R_IsNA(x[idx]) && !R_IsNA(y[idx])) {
        sum_x += x[idx];
        sum_y += y[idx];
        sum_xy += x[idx] * y[idx];
        sum_x2 += x[idx] * x[idx];
        sum_y2 += y[idx] * y[idx];
        count++;
      }
    }
    if(count > 1) {
      double cov = (sum_xy - (sum_x * sum_y) / count);
      double var_x = (sum_x2 - (sum_x * sum_x) / count);
      double var_y = (sum_y2 - (sum_y * sum_y) / count);
      if(var_x > 0 && var_y > 0) {
        result[i] = cov / sqrt(var_x * var_y);
      }
    }
  }
  return result;
}
// [[Rcpp::export]]
IntegerVector cpp_market_regime(const NumericVector& returns,
                               int window = 252,
                               double bull_thresh = 0.10,
                               double bear_thresh = -0.10) {
  int n = returns.size();
  IntegerVector regime(n, 0);
  for(int i = window; i < n; ++i) {
    double cumulative_return = 1.0;
    int valid_count = 0;
    for(int j = 0; j < window; ++j) {
      int idx = i - window + j;
      if(!R_IsNA(returns[idx])) {
        cumulative_return *= (1.0 + returns[idx]);
        valid_count++;
      }
    }
    if(valid_count > window * 0.8) {
      double ann_return = pow(cumulative_return, 252.0 / valid_count) - 1.0;
      if(ann_return > bull_thresh) {
        regime[i] = 1;
      } else if(ann_return < bear_thresh) {
        regime[i] = -1;
      } else {
        regime[i] = 0;
      }
    }
  }
  return regime;
}
// [[Rcpp::export]]
NumericMatrix cpp_clip_matrix(const NumericMatrix& mat,
                             const NumericVector& lower,
                             const NumericVector& upper) {
  int nrow = mat.nrow();
  int ncol = mat.ncol();
  NumericMatrix result(nrow, ncol);
  for(int j = 0; j < ncol; ++j) {
    double l = lower[j];
    double u = upper[j];
    for(int i = 0; i < nrow; ++i) {
      double val = mat(i, j);
      if(val < l) {
        result(i, j) = l;
      } else if(val > u) {
        result(i, j) = u;
      } else {
        result(i, j) = val;
      }
    }
  }
  return result;
}
// [[Rcpp::export]]
NumericVector cpp_dtw_distance(const NumericMatrix& pattern,
                              const NumericMatrix& series,
                              int window = 50) {
  int n_pattern = pattern.nrow();
  int n_series = series.nrow();
  int n_features = pattern.ncol();
  NumericVector distances(n_series, NA_REAL);
  for(int i = window; i < n_series; ++i) {
    double min_dist = R_PosInf;
    for(int start = i - window; start <= i - n_pattern; ++start) {
      if(start >= 0) {
        double dist = 0.0;
        for(int k = 0; k < n_pattern; ++k) {
          for(int f = 0; f < n_features; ++f) {
            double diff = pattern(k, f) - series(start + k, f);
            dist += diff * diff;
          }
        }
        if(dist < min_dist) {
          min_dist = dist;
        }
      }
    }
    if(min_dist < R_PosInf) {
      distances[i] = sqrt(min_dist);
    }
  }
  return distances;
}
')
cat(" Ingesting Multi-Source Market Data...\n")
fetch_data_robust <- function(symbol, retries = 3, fallback_cache = TRUE) {
  for (attempt in 1:retries) {
    tryCatch({
      data <- getSymbols(symbol, src = "yahoo", auto.assign = FALSE)
      if (nrow(data) < 10) stop("Insufficient data rows")
      if (sum(is.na(Ad(data))) > nrow(data) * 0.1) stop("Too many NAs")
      dt <- as.data.table(data)
      setnames(dt, "index", "Date")
      dt[, Date := as.Date(Date)]
      cols <- names(dt)
      price_cols <- grep("(Close|Adjusted)", cols, value = TRUE)
      if (length(price_cols) > 0) {
        setnames(dt, price_cols[1], "Close")
      }
      cat(sprintf("    %s: %d rows | %s - %s\n",
                  symbol, nrow(dt), min(dt$Date), max(dt$Date)))
      return(dt[, .(Date, Close)])
    }, error = function(e) {
      if (attempt == retries) {
        cat(sprintf("     %s failed, using synthetic data\n", symbol))
        n_days <- 252 * 10
        dates <- seq(Sys.Date() - n_days, Sys.Date(), by = "day")
        dates <- dates[!weekdays(dates) %in% c("Saturday", "Sunday")]
        set.seed(123 + which(c("SPY", "VIX", "TLT") == symbol))
        if (symbol %in% c("^GSPC", "SPY")) {
          prices <- 4000 * exp(cumsum(rnorm(length(dates), 0.0005, 0.015)))
        } else if (symbol == "^VIX") {
          prices <- 20 + 8 * sin(seq(0, 8*pi, length.out = length(dates))) +
            rnorm(length(dates), 0, 2)
        } else {
          prices <- 100 * exp(cumsum(rnorm(length(dates), 0.0003, 0.012)))
        }
        return(data.table(Date = dates, Close = prices))
      }
      Sys.sleep(2)
    })
  }
}
cat("   Fetching data (sequential, Windows-safe)...\n")
symbols <- c(SYMBOL, MARKET_INDEX, SECTOR_INDEX, VIX_INDEX, TREASURY_10Y)
data_list <- lapply(symbols, function(sym) {
  cat(sprintf("   Fetching %s...\n", sym))
  fetch_data_robust(sym)
})
names(data_list) <- symbols
cat(sprintf("   Data fetched for: %s\n", paste(names(data_list), collapse = ", ")))
print(lapply(data_list, function(dt) {
  if (is.null(dt)) return("NULL")
  list(class = class(dt), cols = names(dt), nrow = ifelse(is.data.frame(dt), nrow(dt), NA))
}))
for (nm in names(data_list)) {
  dt <- copy(data_list[[nm]])
  if (is.null(dt)) stop(sprintf("Elementul '%s' este NULL", nm))
  if (!"data.table" %in% class(dt)) dt <- as.data.table(dt)
  if (!"Date" %in% names(dt)) {
    if (!is.null(rownames(dt)) && length(rownames(dt)) == nrow(dt)) {
      dt[, Date := as.Date(rownames(dt))]
    } else {
      stop(sprintf("Nu am gasit coloana Date in '%s'. Cols: %s", nm, paste(names(dt), collapse = ",")))
    }
  } else {
    dt[, Date := as.Date(Date)]
  }
  if (!"Close" %in% names(dt)) {
    price_col <- grep("Close|Adjusted", names(dt), value = TRUE)[1]
    if (is.na(price_col)) stop(sprintf("Nu am gasit coloana de pret in '%s'. Cols: %s", nm, paste(names(dt), collapse = ",")))
    names(dt)[names(dt) == price_col] <- "Close"
  }
  safe_nm <- gsub("[^A-Za-z0-9_]", "_", nm)
  new_col <- paste0("Close_", safe_nm)
  names(dt)[names(dt) == "Close"] <- new_col
  data_list[[nm]] <- dt[, c("Date", new_col), with = FALSE]
  cat(sprintf("   [rename] %s -> %s\n", nm, new_col))
}
price_cols_only <- unlist(lapply(data_list, function(d) setdiff(names(d), "Date")))
dup_cols <- unique(price_cols_only[duplicated(price_cols_only)])
if (length(dup_cols) > 0) stop(sprintf("Exista coloane de pret duplicate inainte de merge: %s", paste(dup_cols, collapse = ", ")))
dt_merged <- Reduce(function(x, y) merge(x, y, by = "Date", all = FALSE), data_list)
if (!"data.table" %in% class(dt_merged)) dt_merged <- as.data.table(dt_merged)
setorder(dt_merged, Date)
cat(sprintf(" Merged dataset: %d rows | Features: %d\n", nrow(dt_merged), ncol(dt_merged)))
print(head(dt_merged))
print(tail(dt_merged))
print(sapply(data_list, nrow))
cat("\n  Building Production Feature Matrix...\n")
build_features_production <- function(dt) {
  X <- copy(dt)
  make_safe <- function(s) gsub("[^A-Za-z0-9_]", "_", s)
  col_map <- list(
    Close_Stock  = grep(paste0("^Close_", make_safe(SYMBOL),       "$"), names(X), value=TRUE)[1],
    Close_Market = grep(paste0("^Close_", make_safe(MARKET_INDEX), "$"), names(X), value=TRUE)[1],
    Close_Sector = grep(paste0("^Close_", make_safe(SECTOR_INDEX), "$"), names(X), value=TRUE)[1],
    Close_VIX    = grep(paste0("^Close_", make_safe(VIX_INDEX),    "$"), names(X), value=TRUE)[1],
    Close_10Y    = grep(paste0("^Close_", make_safe(TREASURY_10Y), "$"), names(X), value=TRUE)[1]
  )
  missing <- names(col_map)[sapply(col_map, function(v) is.na(v) || length(v)==0)]
  if (length(missing) > 0)
    stop(sprintf("build_features_production: coloane lipsa: %s\nDisponibile: %s",
                 paste(missing, collapse=", "), paste(names(X), collapse=", ")))
  for (std in names(col_map)) {
    orig <- col_map[[std]]
    if (orig != std) setnames(X, orig, std)
  }
  X[, `:=`(
    Ret_Stock  = c(NA, diff(log(Close_Stock))),
    Ret_Market = c(NA, diff(log(Close_Market))),
    Ret_Sector = c(NA, diff(log(Close_Sector))),
    Ret_VIX    = c(NA, diff(log(Close_VIX))),
    Yield_10Y  = Close_10Y / 100
  )]
  X[, Price_ROC_5  := Close_Stock / shift(Close_Stock, 5,  type="lag") - 1]
  X[, Price_ROC_20 := Close_Stock / shift(Close_Stock, 20, type="lag") - 1]
  X[, Price_ROC_60 := Close_Stock / shift(Close_Stock, 60, type="lag") - 1]
  X[, RS_14 := frollmean(pmax(Ret_Stock,0),14) /
      (frollmean(pmax(Ret_Stock,0),14) + frollmean(pmax(-Ret_Stock,0),14) + 1e-10)]
  X[, TR     := abs(Ret_Stock) * Close_Stock]
  X[, ATR_14 := frollmean(TR, 14, align="right")]
  X[, Norm_ATR := ATR_14 / Close_Stock]
  ret_s <- X$Ret_Stock; ret_m <- X$Ret_Market
  beta_fn  <- function(m) { ok <- !is.na(m[,1])&!is.na(m[,2]); if(sum(ok)>30) cov(m[ok,1],m[ok,2])/var(m[ok,2]) else NA_real_ }
  alpha_fn <- function(m) { ok <- !is.na(m[,1])&!is.na(m[,2]); if(sum(ok)>30) { b<-cov(m[ok,1],m[ok,2])/var(m[ok,2]); mean(m[ok,1]-b*m[ok,2]) } else NA_real_ }
  X[, Beta_60  := zoo::rollapplyr(cbind(ret_s,ret_m), 60, beta_fn,  by.column=FALSE, fill=NA)]
  X[, Alpha_60 := zoo::rollapplyr(cbind(ret_s,ret_m), 60, alpha_fn, by.column=FALSE, fill=NA)]
  X[, Corr_Price_VIX     := cpp_rolling_corr(Ret_Stock, Ret_VIX, 20)]
  X[, VIX_Contango       := shift(Close_VIX, 5, type="lead") / Close_VIX - 1]
  X[, Yield_Spread_10Y2Y := Close_10Y - shift(Close_10Y, 42, type="lag")]
  X[, Mkt_Regime := cpp_market_regime(Ret_Market, 252L, 0.10, -0.10)]
  X[, Vol_20d    := zoo::rollapplyr(Ret_Stock, 20, sd, fill=NA) * sqrt(252)]
  X[, Vol_Regime := fcase(
    Vol_20d > quantile(Vol_20d,0.8,na.rm=TRUE), "HIGH",
    Vol_20d < quantile(Vol_20d,0.2,na.rm=TRUE), "LOW",
    default = "NORMAL"
  )]
  X[, Spread_Proxy    := abs(Ret_Stock)]
  X[, Liquidity_Proxy := 1/(abs(Ret_Stock)+1e-6)]
  X[, `:=`(DayOfWeek=as.integer(format(Date,"%u")), Month=as.integer(format(Date,"%m")),
           Quarter=as.integer((as.integer(format(Date,"%m"))-1)/3)+1, Year=as.integer(format(Date,"%Y")))]
  X[, `:=`(MonthSin=sin(2*pi*Month/12), MonthCos=cos(2*pi*Month/12),
           QuarterSin=sin(2*pi*Quarter/4), QuarterCos=cos(2*pi*Quarter/4))]
  lag_feats <- c("Ret_Stock","Ret_Market","Ret_VIX","Corr_Price_VIX","Spread_Proxy","Norm_ATR","Price_ROC_5","Price_ROC_20")
  for (feat in lag_feats)
    for (lag in c(1,2,3,5,8,13,21))
      X[, paste0(feat,"_lag",lag) := shift(get(feat), lag, type="lag")]
  X[, `:=`(VIX_Beta_Interaction = Corr_Price_VIX*Beta_60,
           Vol_Regime_Beta      = (Vol_Regime=="HIGH")*Beta_60,
           Sentiment_Momentum   = Ret_VIX*Ret_Market)]
  X[, Target_20d    := shift(Close_Stock,-FORECAST_DAYS,type="lead") / Close_Stock - 1]
  X[, Target_Binary := as.integer(Target_20d > NEUTRAL_BAND)]
  for (col in c("Ret_Stock","Ret_Market","Ret_VIX","Corr_Price_VIX","Vol_20d","Beta_60","Target_20d"))
    X <- X[!is.na(get(col))]
  cat(sprintf("   \u2713 Feature matrix: %d rows x %d columns\n", nrow(X), ncol(X)))
  return(X)
}
dt_features <- build_features_production(dt_merged)
cat("\n\U0001f680 Initializing Pure Rolling Walk-Forward Engine...\n")
exclude_cols <- c("Date", "Close_Stock", "Close_Market", "Close_Sector",
                  "Close_VIX", "Close_10Y", "TR",
                  "Target_20d", "Target_Binary", "Vol_Regime")
feature_cols <- setdiff(names(dt_features), exclude_cols)
X_mat <- as.matrix(dt_features[, ..feature_cols])
y_reg <- dt_features$Target_20d * 100
y_cls <- dt_features$Target_Binary
dates <- dt_features$Date
n_total <- nrow(X_mat)
fold_size <- floor((n_total - TRAIN_WINDOW) / N_FOLDS)
test_indices <- lapply(1:N_FOLDS, function(i) {
  test_start <- TRAIN_WINDOW + (i - 1) * fold_size + 1
  test_end <- min(test_start + fold_size - 1, n_total)
  train_end <- test_start - 1
  train_start <- train_end - TRAIN_WINDOW + 1
  if (train_start < 1 || test_end > n_total ||
      (test_end - test_start + 1) < FORECAST_DAYS) {
    return(NULL)
  }
  return(list(
    train_idx = train_start:train_end,
    test_idx = test_start:test_end
  ))
})
test_indices <- test_indices[!sapply(test_indices, is.null)]
cat(sprintf("    %d pure rolling folds configured\n", length(test_indices)))
cat("\n Initializing Multi-Model Ensemble...\n")
cat("\n Initializing Multi-Model Ensemble (XGB + LGBM Only)...\n")
ensemble_configs <- list(
  xgb_huber = list(
    type = "xgboost",
    params = list(
      objective = "reg:pseudohubererror",
      eval_metric = "rmse",
      max_depth = 6,
      eta = 0.02,
      subsample = 0.8,
      colsample_bytree = 0.8,
      lambda = 2.0,
      alpha = 0.5,
      gamma = 0.2,
      min_child_weight = 5,
      tree_method = "hist"
    ),
    rounds = 1000
  ),
  xgb_quantile = list(
    type = "xgboost",
    params = list(
      objective = "reg:quantileerror",
      eval_metric = "quantile",
      quantile_alpha = 0.5,
      max_depth = 5,
      eta = 0.03,
      subsample = 0.75,
      colsample_bytree = 0.75,
      lambda = 1.5,
      alpha = 0.3,
      tree_method = "hist"
    ),
    rounds = 1000
  ),
  lgb_fast = list(
    type = "lightgbm",
    params = list(
      objective = "regression",
      metric = "rmse",
      num_leaves = 31,
      learning_rate = 0.05,
      feature_fraction = 0.8,
      bagging_fraction = 0.8,
      bagging_freq = 5,
      lambda_l1 = 0.2,
      lambda_l2 = 0.5,
      min_data_in_leaf = 20,
      device_type = "cpu"
    ),
    rounds = 800
  )
)
cat(" Building Bayesian Uncertainty Engine...\n")
bayesian_uncertainty <- function(predictions_list) {
  n_models <- length(predictions_list)
  n_samples <- length(predictions_list[[1]])
  pred_matrix <- do.call(cbind, predictions_list)
  ensemble_mean <- rowMeans(pred_matrix, na.rm = TRUE)
  ensemble_sd <- apply(pred_matrix, 1, sd, na.rm = TRUE)
  weights <- rep(1/n_models, n_models)
  lower_95 <- ensemble_mean - 1.96 * ensemble_sd
  upper_95 <- ensemble_mean + 1.96 * ensemble_sd
  uncertainty_score <- ensemble_sd / (abs(ensemble_mean) + 1e-10)
  return(list(
    mean = ensemble_mean,
    sd = ensemble_sd,
    lower_95 = lower_95,
    upper_95 = upper_95,
    uncertainty = uncertainty_score,
    weights = weights
  ))
}
cat("\n  Starting Walk-Forward Training (Multi-Model Ensemble)...\n")
ensemble_results <- list()
signal_journal <- data.table()
cat(sprintf("\n Incepand Walk-Forward (%d folduri, secvential)...\n", length(test_indices)))
ensemble_results <- vector("list", length(test_indices))
for (fold_idx in seq_along(test_indices)) {
  cat(sprintf("   Fold %d/%d...\n", fold_idx, length(test_indices)))
  fold_result <- tryCatch({
    fold <- test_indices[[fold_idx]]
    X_train <- X_mat[fold$train_idx, ]
    y_train_reg <- y_reg[fold$train_idx]
    X_test  <- X_mat[fold$test_idx, , drop = FALSE]
    y_test_reg <- y_reg[fold$test_idx]
    fold_dates <- dates[fold$test_idx]
    train_median <- apply(X_train, 2, median, na.rm = TRUE)
    train_iqr    <- apply(X_train, 2, IQR,    na.rm = TRUE)
    train_iqr[train_iqr == 0] <- 1
    X_train_scaled <- scale(X_train, center = train_median, scale = train_iqr)
    X_test_scaled  <- scale(X_test,  center = train_median, scale = train_iqr)
    q_low  <- apply(X_train, 2, quantile, 0.01, na.rm = TRUE)
    q_high <- apply(X_train, 2, quantile, 0.99, na.rm = TRUE)
    X_train_scaled <- cpp_clip_matrix(X_train_scaled, q_low, q_high)
    X_test_scaled  <- cpp_clip_matrix(X_test_scaled,  q_low, q_high)
    n_tr    <- nrow(X_train_scaled)
    val_idx <- round(n_tr * 0.8):n_tr
    tr_idx  <- 1:(round(n_tr * 0.8) - 1)
    dtr  <- xgb.DMatrix(X_train_scaled[tr_idx, ],  label = y_train_reg[tr_idx])
    dval <- xgb.DMatrix(X_train_scaled[val_idx, ], label = y_train_reg[val_idx])
    dtest_xgb <- xgb.DMatrix(X_test_scaled)
    xgb_huber_m <- xgb.train(
      params = ensemble_configs$xgb_huber$params, data = dtr,
      nrounds = ensemble_configs$xgb_huber$rounds,
      early_stopping_rounds = 50, evals = list(val = dval), verbose = 0)
    xgb_quant_m <- xgb.train(
      params = ensemble_configs$xgb_quantile$params, data = dtr,
      nrounds = ensemble_configs$xgb_quantile$rounds,
      early_stopping_rounds = 50, evals = list(val = dval), verbose = 0)
    dtrain_lgb <- lgb.Dataset(X_train_scaled[tr_idx, ],  label = y_train_reg[tr_idx])
    dval_lgb   <- lgb.Dataset(X_train_scaled[val_idx, ], label = y_train_reg[val_idx])
    lgb_m <- lgb.train(
      params = ensemble_configs$lgb_fast$params, data = dtrain_lgb,
      nrounds = ensemble_configs$lgb_fast$rounds,
      valids = list(val = dval_lgb), early_stopping_rounds = 50, verbose = -1)
    preds <- list(
      xgb_huber   = predict(xgb_huber_m, dtest_xgb) / 100,
      xgb_quantile = predict(xgb_quant_m, dtest_xgb) / 100,
      lgb = predict(lgb_m, X_test_scaled) / 100
    )
    unc <- bayesian_uncertainty(preds)
    list(fold = fold_idx, dates = fold_dates,
         ensemble_mean = unc$mean, ensemble_uncertainty = unc$uncertainty,
         actual = y_test_reg / 100)
  }, error = function(e) {
    cat(sprintf("   Fold %d esuat: %s\n", fold_idx, conditionMessage(e)))
    NULL
  })
  ensemble_results[[fold_idx]] <- fold_result
}
ensemble_results <- ensemble_results[!sapply(ensemble_results, is.null)]
cat(" Compiling Walk-Forward Results...\n")
all_dates <- unlist(lapply(ensemble_results, function(x) x$dates))
all_predictions <- unlist(lapply(ensemble_results, function(x) x$ensemble_mean))
all_uncertainty <- unlist(lapply(ensemble_results, function(x) x$ensemble_uncertainty))
all_actual <- unlist(lapply(ensemble_results, function(x) x$actual))
signal_journal <- data.table(
  Date = as.Date(all_dates),
  Pred_Ret = all_predictions,
  Uncertainty = all_uncertainty,
  Actual_Ret = all_actual
)
cat("\n Running Event-Based Backtest with Trading Calendar...\n")
create_trading_calendar <- function(start_date, end_date) {
  all_dates <- seq(start_date, end_date, by = "day")
  weekdays <- all_dates[!weekdays(all_dates) %in% c("Saturday", "Sunday")]
  holidays <- as.Date(c(
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-03-29",
    "2024-05-27", "2024-06-19", "2024-07-04", "2024-09-02",
    "2024-10-14", "2024-11-11", "2024-11-28", "2024-12-25"
  ))
  trading_days <- weekdays[!weekdays %in% holidays]
  return(trading_days)
}
trading_calendar <- create_trading_calendar(min(dt_features$Date), max(dt_features$Date))
find_next_trading_day <- function(start_date, n_days_ahead, calendar) {
  start_idx <- which(calendar == start_date)
  if (length(start_idx) == 0) {
    start_idx <- which(calendar >= start_date)[1]
  }
  if (is.na(start_idx) || (start_idx + n_days_ahead - 1) > length(calendar)) {
    return(NA)
  }
  return(calendar[start_idx + n_days_ahead - 1])
}
generate_trades <- function(signal_journal, price_data, forecast_days) {
  trades <- list()
  trade_id <- 1
  trading_signals <- merge(signal_journal,
                           price_data[, .(Date, Close = Close_Stock)],
                           by = "Date", all.x = TRUE)
  setorder(trading_signals, Date)
  for(i in 1:nrow(trading_signals)) {
    signal_row <- trading_signals[i]
    entry_signal <- signal_row$Pred_Ret > NEUTRAL_BAND
    low_uncertainty <- signal_row$Uncertainty < 0.5
    if(entry_signal && low_uncertainty) {
      entry_date <- signal_row$Date
      exit_date <- find_next_trading_day(entry_date, forecast_days, trading_calendar)
      if(!is.na(exit_date)) {
        exit_price <- price_data[Date == exit_date, Close_Stock]
        if(length(exit_price) > 0 && !is.na(exit_price)) {
          entry_price <- signal_row$Close
          trade_return <- (exit_price / entry_price) - 1
          trades[[trade_id]] <- data.table(
            Trade_ID = trade_id,
            Entry_Date = entry_date,
            Exit_Date = exit_date,
            Entry_Price = entry_price,
            Exit_Price = exit_price,
            Predicted_Return = signal_row$Pred_Ret,
            Actual_Return = trade_return,
            Uncertainty = signal_row$Uncertainty,
            Holding_Days = as.numeric(exit_date - entry_date)
          )
          trade_id <- trade_id + 1
        }
      }
    }
  }
  if(length(trades) > 0) {
    return(rbindlist(trades))
  } else {
    return(data.table())
  }
}
trade_journal <- generate_trades(signal_journal, dt_features, FORECAST_DAYS)
cat(sprintf("    Generated %d trades with exact holding periods\n", nrow(trade_journal)))
cat("\n Running Portfolio Simulation with Dynamic Position Sizing...\n")
run_portfolio_simulation_pro <- function(trade_journal, price_data,
                                         initial_capital = 100000,
                                         max_position_pct = 0.20,
                                         commission = 0.001,
                                         stop_loss_pct = 0.08,
                                         take_profit_pct = 0.15) {
  all_dates <- unique(c(trade_journal$Entry_Date, trade_journal$Exit_Date))
  timeline <- data.table(
    Date = seq(min(all_dates), max(all_dates), by = "day")
  )
  timeline <- merge(timeline, price_data[, .(Date, Price = Close_Stock)],
                    by = "Date", all.x = TRUE)
  timeline[, Price := nafill(Price, type = "locf")]
  timeline[, Price := nafill(Price, type = "nocb")]
  portfolio <- data.table(
    Date = timeline$Date,
    Cash = initial_capital,
    Equity = initial_capital,
    Positions = 0,
    Market_Value = 0,
    Total_Exposure = 0,
    Drawdown = 0
  )
  active_positions <- data.table(
    Position_ID = integer(),
    Entry_Date = as.Date(character()),
    Exit_Date = as.Date(character()),
    Entry_Price = numeric(),
    Shares = numeric(),
    Stop_Loss = numeric(),
    Take_Profit = numeric()
  )
  max_equity <- initial_capital
  for(i in 1:nrow(timeline)) {
    current_date  <- timeline$Date[i]
    current_price <- timeline$Price[i]
    if (is.na(current_price) || current_price <= 0) next
    if(nrow(active_positions) > 0) {
      positions_to_close <- c()
      for(pos_idx in 1:nrow(active_positions)) {
        pos <- active_positions[pos_idx]
        if(!is.na(pos$Stop_Loss) && current_price <= pos$Stop_Loss) {
          positions_to_close <- c(positions_to_close, pos_idx)
          next
        }
        if(!is.na(pos$Take_Profit) && current_price >= pos$Take_Profit) {
          positions_to_close <- c(positions_to_close, pos_idx)
          next
        }
        if(!is.na(pos$Exit_Date) && current_date >= pos$Exit_Date) {
          positions_to_close <- c(positions_to_close, pos_idx)
          next
        }
      }
      if (length(positions_to_close) > 0) {
        total_proceeds <- sum(sapply(positions_to_close, function(idx) {
          active_positions[idx, Shares] * current_price * (1 - commission)
        }))
        portfolio[i, Cash := Cash + total_proceeds]
        for (idx in sort(positions_to_close, decreasing = TRUE)) {
          if (idx <= nrow(active_positions)) {
            active_positions <- active_positions[-idx]
          }
        }
      }
    }
    todays_entries <- trade_journal[Entry_Date == current_date]
    if(nrow(todays_entries) > 0) {
      for(entry_idx in 1:nrow(todays_entries)) {
        trade <- todays_entries[entry_idx]
        current_equity <- portfolio[i, Equity]
        confidence_multiplier <- 1 - trade$Uncertainty
        kelly_fraction <- confidence_multiplier * max_position_pct
        max_trade_capital <- current_equity * kelly_fraction
        if(nrow(active_positions) < 5) {
          shares <- floor(max_trade_capital / current_price)
          if(shares > 0) {
            cost <- shares * current_price * (1 + commission)
            if(portfolio[i, Cash] >= cost) {
              portfolio[i, Cash := Cash - cost]
              stop_loss_price <- current_price * (1 - stop_loss_pct)
              take_profit_price <- current_price * (1 + take_profit_pct)
              new_position <- data.table(
                Position_ID = nrow(active_positions) + 1,
                Entry_Date = current_date,
                Exit_Date = trade$Exit_Date,
                Entry_Price = current_price,
                Shares = shares,
                Stop_Loss = stop_loss_price,
                Take_Profit = take_profit_price
              )
              active_positions <- rbind(active_positions, new_position)
            }
          }
        }
      }
    }
    market_value <- 0
    if(nrow(active_positions) > 0) {
      for(pos_idx in 1:nrow(active_positions)) {
        pos <- active_positions[pos_idx]
        market_value <- market_value + (pos$Shares * current_price)
      }
    }
    total_equity <- portfolio[i, Cash] + market_value
    portfolio[i, `:=`(
      Market_Value = market_value,
      Equity = total_equity,
      Positions = nrow(active_positions),
      Total_Exposure = market_value / total_equity
    )]
    if(total_equity > max_equity) {
      max_equity <- total_equity
    }
    portfolio[i, Drawdown := (total_equity - max_equity) / max_equity]
  }
  return(list(
    portfolio = portfolio,
    active_positions = active_positions
  ))
}
portfolio_results <- run_portfolio_simulation_pro(
  trade_journal,
  dt_features,
  initial_capital = 100000,
  max_position_pct = 0.20
)
portfolio_dt <- portfolio_results$portfolio
cat("\n Calculating Advanced Performance Metrics...\n")
calculate_performance_metrics <- function(portfolio_dt, trade_journal) {
  equity_curve <- portfolio_dt$Equity
  returns <- c(NA, diff(log(equity_curve)))
  returns <- returns[!is.na(returns)]
  total_return <- (tail(equity_curve, 1) / equity_curve[1]) - 1
  annual_return <- (1 + total_return)^(252 / length(returns)) - 1
  annual_vol <- sd(returns, na.rm = TRUE) * sqrt(252)
  sharpe_ratio <- ifelse(annual_vol > 0, annual_return / annual_vol, 0)
  drawdowns <- portfolio_dt$Drawdown
  max_drawdown <- min(drawdowns)
  calmar_ratio <- ifelse(abs(max_drawdown) > 0, annual_return / abs(max_drawdown), 0)
  if(nrow(trade_journal) > 0) {
    trade_returns <- trade_journal$Actual_Return
    win_rate <- mean(trade_returns > 0, na.rm = TRUE) * 100
    avg_win <- mean(trade_returns[trade_returns > 0], na.rm = TRUE)
    avg_loss <- mean(trade_returns[trade_returns < 0], na.rm = TRUE)
    profit_factor <- ifelse(abs(sum(trade_returns[trade_returns < 0])) > 0,
                            abs(sum(trade_returns[trade_returns > 0]) /
                                  sum(trade_returns[trade_returns < 0])), 0)
  } else {
    win_rate <- avg_win <- avg_loss <- profit_factor <- 0
  }
  if("Uncertainty" %in% names(trade_journal)) {
    high_conf_trades <- trade_journal[Uncertainty < 0.3]
    if(nrow(high_conf_trades) > 0) {
      high_conf_return <- mean(high_conf_trades$Actual_Return, na.rm = TRUE)
    } else {
      high_conf_return <- 0
    }
  } else {
    high_conf_return <- 0
  }
  metrics <- list(
    total_return = total_return,
    annual_return = annual_return,
    annual_volatility = annual_vol,
    sharpe_ratio = sharpe_ratio,
    max_drawdown = max_drawdown,
    calmar_ratio = calmar_ratio,
    win_rate = win_rate,
    avg_win = avg_win,
    avg_loss = avg_loss,
    profit_factor = profit_factor,
    total_trades = nrow(trade_journal),
    high_confidence_return = high_conf_return
  )
  return(metrics)
}
metrics <- calculate_performance_metrics(portfolio_dt, trade_journal)
cat("\n Generating Live Trading Signal...\n")
cat("   Training final ensemble on full dataset...\n")
X_final <- X_mat
y_final <- y_reg
final_median <- apply(X_final, 2, median, na.rm = TRUE)
final_iqr <- apply(X_final, 2, IQR, na.rm = TRUE)
final_iqr[final_iqr == 0] <- 1
X_final_scaled <- scale(X_final, center = final_median, scale = final_iqr)
q_low_final <- apply(X_final, 2, quantile, 0.01, na.rm = TRUE)
q_high_final <- apply(X_final, 2, quantile, 0.99, na.rm = TRUE)
X_final_scaled <- cpp_clip_matrix(X_final_scaled, q_low_final, q_high_final)
dtrain_final <- xgb.DMatrix(X_final_scaled, label = y_final)
final_xgb <- xgb.train(
  params = ensemble_configs$xgb_huber$params,
  data = dtrain_final,
  nrounds = 2000,
  verbose = 0
)
latest_features <- X_mat[nrow(X_mat), , drop = FALSE]
latest_scaled <- scale(latest_features, center = final_median, scale = final_iqr)
latest_scaled <- cpp_clip_matrix(latest_scaled, q_low_final, q_high_final)
latest_pred_xgb <- predict(final_xgb, xgb.DMatrix(latest_scaled)) / 100
current_row <- dt_features[.N]
current_price <- current_row$Close_Stock
current_vix_corr <- current_row$Corr_Price_VIX
current_vol_regime <- current_row$Vol_Regime
current_mkt_regime <- current_row$Mkt_Regime
cat(paste0("\n", strrep("=", 100)))
cat("\n LIVE TRADING SIGNAL - PRODUCTION GRADE\n")
cat(paste0(strrep("=", 100), "\n"))
cat(sprintf("    Symbol: %s | Current Price: $%.2f\n", SYMBOL, current_price))
cat(sprintf("    Predicted %d-Day Return: %+.2f%%\n",
            FORECAST_DAYS, latest_pred_xgb * 100))
cat(sprintf("    Target Price: $%.2f\n", current_price * (1 + latest_pred_xgb)))
cat("\n MARKET CONTEXT ANALYSIS:\n")
cat(sprintf("    VIX Correlation: %.3f %s\n",
            current_vix_corr,
            ifelse(current_vix_corr > 0.3, " (HIGH RISK)",
                   ifelse(current_vix_corr > 0.2, " (ELEVATED)", " (NORMAL)"))))
cat(sprintf("    Market Regime: %s\n",
            ifelse(current_mkt_regime == 1, " BULL",
                   ifelse(current_mkt_regime == -1, " BEAR", " NEUTRAL"))))
cat(sprintf("    Volatility Regime: %s\n", current_vol_regime))
if(latest_pred_xgb > NEUTRAL_BAND * 1.5 &&
   current_vix_corr < 0.2 &&
   current_mkt_regime >= 0) {
  cat("\n SIGNAL: STRONG BUY\n")
  cat("    High conviction setup\n")
  cat("    Favorable market regime\n")
  cat("    Low VIX correlation risk\n")
  rec_size <- min(0.25, 0.15 + (latest_pred_xgb - NEUTRAL_BAND) * 2)
  cat(sprintf("    Recommended Allocation: %.1f%% of portfolio\n", rec_size * 100))
} else if(latest_pred_xgb > NEUTRAL_BAND &&
          current_vix_corr < 0.3) {
  cat("\n SIGNAL: MODERATE BUY\n")
  cat("    Positive expected return\n")
  cat("    Acceptable risk profile\n")
  cat(sprintf("    Recommended Allocation: %.1f%% of portfolio\n", 0.10 * 100))
} else if(latest_pred_xgb < -NEUTRAL_BAND ||
          current_vix_corr > 0.3) {
  cat("\n SIGNAL: SELL/AVOID\n")
  if(current_vix_corr > 0.3) {
    cat("     CRITICAL: High VIX correlation detected\n")
    cat("    Market anomaly present\n")
    cat("    Elevated crash risk\n")
  } else {
    cat("    Negative expected return\n")
  }
} else {
  cat("\n SIGNAL: NEUTRAL/HOLD\n")
  cat("    Expected return within noise band\n")
  cat("    Wait for better opportunity\n")
}
cat("\n Generating Production Visualizations...\n")
p1 <- ggplot(portfolio_dt, aes(x = Date)) +
  geom_line(aes(y = Equity), color = "darkgreen", size = 1) +
  geom_area(aes(y = Equity), fill = "darkgreen", alpha = 0.1) +
  labs(title = "Portfolio Equity Curve (Dynamic Position Sizing)",
       subtitle = sprintf("Final: $%s | Sharpe: %.2f | Max DD: %.1f%%",
                          format(round(tail(portfolio_dt$Equity, 1)), big.mark = ","),
                          metrics$sharpe_ratio,
                          metrics$max_drawdown * 100),
       y = "Equity ($)") +
  scale_y_continuous(labels = scales::dollar_format()) +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold"))
p2 <- ggplot(portfolio_dt, aes(x = Date, y = Drawdown)) +
  geom_area(fill = "darkred", alpha = 0.3) +
  geom_line(color = "darkred", size = 0.5) +
  geom_hline(yintercept = 0, color = "black", size = 0.3) +
  labs(title = "Portfolio Drawdown",
       y = "Drawdown (%)") +
  scale_y_continuous(labels = scales::percent_format()) +
  theme_minimal()
plot_data <- merge(signal_journal,
                   dt_features[, .(Date, Close_Stock)],
                   by = "Date")
plot_data[, Actual_Future_Ret := shift(Close_Stock, -FORECAST_DAYS, type = "lead") /
            Close_Stock - 1]
plot_data <- na.omit(plot_data)
plot_data <- plot_data[Date >= max(Date) - 365 * 2]
p3 <- ggplot(plot_data, aes(x = Date)) +
  geom_line(aes(y = Actual_Future_Ret * 100, color = "Actual"), alpha = 0.6, size = 0.8) +
  geom_line(aes(y = Pred_Ret * 100, color = "Predicted"), alpha = 0.8, size = 0.8) +
  geom_ribbon(aes(ymin = (Pred_Ret - Uncertainty) * 100,
                  ymax = (Pred_Ret + Uncertainty) * 100),
              fill = "red", alpha = 0.1) +
  scale_color_manual(values = c("Actual" = "gray50", "Predicted" = "darkred")) +
  labs(title = "Ensemble Model Predictions vs Actual Returns",
       subtitle = sprintf("RMSE: %.2f%% | Direction Accuracy: %.1f%%",
                          sqrt(mean((plot_data$Actual_Future_Ret - plot_data$Pred_Ret)^2)) * 100,
                          mean(sign(plot_data$Actual_Future_Ret) == sign(plot_data$Pred_Ret)) * 100),
       x = "Date", y = "Return (%)",
       color = "") +
  theme_minimal() +
  theme(legend.position = "top")
if(nrow(trade_journal) > 0) {
  p4 <- ggplot(trade_journal, aes(x = Actual_Return * 100)) +
    geom_histogram(bins = 30, fill = "steelblue", alpha = 0.7) +
    geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
    labs(title = "Trade Return Distribution",
         subtitle = sprintf("Win Rate: %.1f%% | Avg Win: +%.2f%% | Avg Loss: %.2f%%",
                            metrics$win_rate,
                            metrics$avg_win * 100,
                            metrics$avg_loss * 100),
         x = "Trade Return (%)", y = "Count") +
    theme_minimal()
}
print(p1)
print(p2)
print(p3)
if(exists("p4")) print(p4)
cat(paste0("\n", strrep("=", 100)))
cat("\n ENGINE_SCRIPT - FINAL REPORT")
cat(paste0("\n", strrep("=", 100)))
cat("\n\n PERFORMANCE SUMMARY:\n")
cat(sprintf("    Total Return: %.2f%%\n", metrics$total_return * 100))
cat(sprintf("    Annual Return: %.2f%%\n", metrics$annual_return * 100))
cat(sprintf("    Annual Volatility: %.2f%%\n", metrics$annual_volatility * 100))
cat(sprintf("    Sharpe Ratio: %.2f\n", metrics$sharpe_ratio))
cat(sprintf("    Max Drawdown: %.2f%%\n", metrics$max_drawdown * 100))
cat(sprintf("    Calmar Ratio: %.2f\n", metrics$calmar_ratio))
cat("\n TRADE STATISTICS:\n")
cat(sprintf("    Total Trades: %d\n", metrics$total_trades))
cat(sprintf("    Win Rate: %.1f%%\n", metrics$win_rate))
cat(sprintf("    Profit Factor: %.2f\n", metrics$profit_factor))
cat(sprintf("    Average Win: +%.2f%%\n", metrics$avg_win * 100))
cat(sprintf("    Average Loss: %.2f%%\n", metrics$avg_loss * 100))
cat("\n MODEL STATISTICS:\n")
cat(sprintf("    Ensemble Size: %d models\n", length(ensemble_configs)))
cat(sprintf("    Feature Count: %d\n", length(feature_cols)))
cat(sprintf("    Training Window: %d days\n", TRAIN_WINDOW))
cat(sprintf("    Walk-Forward Folds: %d\n", N_FOLDS))
cat("\n  RISK ASSESSMENT:\n")
if(current_vix_corr > 0.3) {
  cat("    HIGH RISK: Elevated VIX correlation detected\n")
} else if(current_vix_corr > 0.2) {
  cat("     MEDIUM RISK: Moderate VIX correlation\n")
} else {
  cat("    LOW RISK: Normal VIX correlation\n")
}
if(metrics$max_drawdown < -0.20) {
  cat("    HIGH DRAWDOWN: Exceeds 20% tolerance\n")
} else if(metrics$max_drawdown < -0.10) {
  cat("     MODERATE DRAWDOWN: Within 10-20% range\n")
} else {
  cat("    ACCEPTABLE DRAWDOWN: Below 10%\n")
}
cat(paste0("\n", strrep("=", 100)))
cat("\n ENGINE_SCRIPT v3.0 - EXECUTION COMPLETE")
cat(paste0(strrep("=", 100), "\n\n"))
cat(" Exporting results for production use...\n")
saveRDS(final_xgb, "production_xgb_model.rds")
saveRDS(ensemble_configs, "ensemble_configs.rds")
fwrite(portfolio_dt, "portfolio_history.csv")
fwrite(trade_journal, "trade_journal.csv")
cat(" All artifacts saved. Model ready for production deployment.\n")
