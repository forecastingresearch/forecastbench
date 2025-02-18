rm(list = ls())
library(rvest)
library(janitor)
library(reticulate)
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)
library(ggplot2)
library(ggrepel)
library(scales)
pd <- import("pandas")
io <- import("io")

human_leaderboard_url <- "https://github.com/forecastingresearch/forecastbench-datasets/raw/refs/heads/main/leaderboards/csv/human_leaderboard_overall.csv"

# Get leaderboard
response <- GET(human_leaderboard_url)
stop_for_status(response)

# Convert response content to a data frame
human_leaderboard <- read.csv(textConnection(content(response, as = "text")), stringsAsFactors = FALSE)

human_leaderboard <- human_leaderboard %>%
  clean_names()
names(human_leaderboard) <- gsub("_n_\\d{1,3}(_\\d{3})*\\b", "", names(human_leaderboard))

supers_brier <- human_leaderboard %>%
  filter(model == "Superforecaster median forecast") %>%
  select(overall_score) %>%
  unlist()

# Import arena
repo_owner <- "lmarena-ai"
repo_name <- "chatbot-arena-leaderboard"
branch <- "main"

# Fetch list of files in repo
api_url <- paste0("https://huggingface.co/api/spaces/", repo_owner, "/", repo_name, "/tree/", branch)
response <- GET(api_url)
stop_for_status(response)
file_list <- content(response, as = "parsed", simplifyVector = TRUE)

# Filter to elo_results .pkl files
pickle_files <- file_list %>%
  filter(grepl("elo_results", path)) %>%
  rowwise() %>%
  # Get dates from .pkl paths
  mutate(date = ymd(
    gsub(
      ".pkl",
      "",
      gsub("elo_results_", "", path)
    )
  ))

most_recent_pkl <- pickle_files %>%
  ungroup() %>%
  filter(date == max(date))

# Download pkl
download_url <- paste0("https://huggingface.co/spaces/", repo_owner, "/", repo_name, "/resolve/", branch, "/", most_recent_pkl$path, "?download=true")

response <- GET(download_url)
stop_for_status(response)

pickle_data <- content(response, "raw")

pickle_obj <- pd$read_pickle(io$BytesIO(pickle_data))

arena <- pickle_obj$text$full$leaderboard_table_df %>%
  arrange(final_ranking)
arena <- arena %>%
  mutate(model = rownames((arena)))
rownames(arena) <- NULL

# Get unique model names
arena_models <- unique(arena$model)
# leaderboard_models <- unique(c(human_leaderboard$model, llm_leaderboard$model))
leaderboard_models <- unique(human_leaderboard$model)
models <- data.frame(arena_models,
  leaderboard_models = c(leaderboard_models, rep(NA, length(arena_models) - length(leaderboard_models))),
  arena = rep(NA, length(arena_models)),
  leaderboard = rep(NA, length(arena_models))
)

# Import model names table
models <- read.csv("model_names.csv")
arena <- arena %>%
  filter(model %in% models$arena)
human_leaderboard <- human_leaderboard %>%
  filter(model %in% models$leaderboard)

# attach arena_model_name and score to leaderboards
human_leaderboard <- human_leaderboard %>%
  rowwise() %>%
  mutate(arena_model_name = unique(models[models$leaderboard == model, ]$arena)) %>%
  mutate(arena_score = arena[arena$model == arena_model_name, ]$rating)

# Separate parentheticals from models
parenthetical_separation <- function(leaderboard) {
  leaderboard$prompt <- NA
  leaderboard$freeze_values <- NA
  leaderboard$news <- NA
  for (i in 1:nrow(leaderboard)) {
    # Get prompt
    if (grepl("scratchpad", leaderboard$model[i])) {
      prompt <- "scratchpad"
    } else if (grepl("zero shot", leaderboard$model[i])) {
      prompt <- "zero shot"
    } else if (grepl("superforecaster", leaderboard$model[i])) {
      prompt <- "superforecaster"
    } else {
      print("No prompt found")
      prompt <- "?"
    }
    # Get freeze values
    if (grepl("freeze values", leaderboard$model[i])) {
      freeze_values <- TRUE
    } else {
      freeze_values <- FALSE
    }
    # Get news
    if (grepl("news", leaderboard$model[i])) {
      news <- TRUE
      if (prompt == "superforecaster") {
        if (grepl("news 1", leaderboard$model[i])) {
          prompt <- paste(prompt, "1")
        } else if (grepl("news 2", leaderboard$model[i])) {
          prompt <- paste(prompt, "2")
        } else if (grepl("news 3", leaderboard$model[i])) {
          prompt <- paste(prompt, "3")
        }
      }
    } else {
      news <- FALSE
    }
    # Add results to leaderboard
    leaderboard$prompt[i] <- prompt
    leaderboard$freeze_values[i] <- freeze_values
    leaderboard$news[i] <- news
    # Get rid of parentheticals from model names
    leaderboard$model[i] <- gsub(" \\(.*?\\)", "", leaderboard$model[i])
  }
  return(leaderboard)
}

human_leaderboard <- parenthetical_separation(human_leaderboard)
# llm_leaderboard <- parenthetical_separation(llm_leaderboard)

# Add in pretty model names
add_pretty_model_names <- function(leaderboard) {
  leaderboard <- leaderboard %>%
    rowwise() %>%
    mutate(pretty_name = unique(models[models$arena == arena_model_name, ]$pretty_name))
}

human_leaderboard <- add_pretty_model_names(human_leaderboard)
# llm_leaderboard <- add_pretty_model_names(llm_leaderboard)

# Produce graphs
leaderboard_arena_graphs <- function(leaderboard) {
  leaderboard <- leaderboard %>%
    filter(prompt == "scratchpad") %>%
    filter(freeze_values == TRUE) %>%
    filter(news == FALSE)

  # Fit a linear model to find the equation of the smooth line
  lm_fit <- lm(overall_score ~ arena_score, data = leaderboard)

  # Extract coefficients
  intercept <- coef(lm_fit)[1]
  slope <- coef(lm_fit)[2]

  print(summary(lm_fit))

  # Calculate the x-coordinate where y = 0.091
  y_intercept <- supers_brier
  x_intersect <- (y_intercept - intercept) / slope

  # Bootstrap to calculate confidence intervals for the intersection
  set.seed(123) # For reproducibility
  n_bootstraps <- 1000
  bootstrap_intersects <- replicate(n_bootstraps, {
    # Resample the data with replacement
    sample_data <- leaderboard[sample(1:nrow(leaderboard), size = nrow(leaderboard), replace = TRUE), ]

    # Fit the linear model to the resampled data
    lm_boot <- lm(overall_score ~ arena_score, data = sample_data)

    # Extract coefficients
    intercept_boot <- coef(lm_boot)[1]
    slope_boot <- coef(lm_boot)[2]

    # Calculate the x-coordinate of the intersection
    (y_intercept - intercept_boot) / slope_boot
  })

  # Compute the 95% confidence interval for the intersections
  ci_lower <- quantile(bootstrap_intersects, 0.025)
  ci_upper <- quantile(bootstrap_intersects, 0.975)
  print(ci_lower)
  print(ci_upper)

  label_data <- data.frame(
    x = x_intersect,
    y = y_intercept,
    label = "Superforecasters"
  )

  p <- ggplot(leaderboard, aes(y = overall_score, x = arena_score)) +
    geom_point(size = 3, color = "#F8766D") + # Adjust point size
    geom_abline(
      intercept = intercept, slope = slope,
      color = "gray", linetype = "dashed"
    ) + # Linear fit line
    geom_text_repel(aes(label = pretty_name), size = 3, max.overlaps = Inf, box.padding = 0.75) +
    geom_hline(yintercept = supers_brier, linetype = "dotted", color = "blue", size = 1) + # Horizontal line
    geom_point(aes(x = x_intersect, y = y_intercept), color = "red", size = 3) + # Intersection point
    geom_errorbarh(
      aes(
        xmin = ci_lower,
        xmax = ci_upper,
        y = y_intercept
      ),
      color = "red", height = 0.01, size = 1, alpha = 0.03
    ) +
    geom_text_repel(
      data = label_data,
      aes(x = x, y = y, label = label),
      color = "red",
      segment.color = "black",
      size = 3,
      hjust = -0.1, # Optional adjustment
      vjust = -1.5 # Optional adjustment
    ) +
    # labs(
    #   title = "Model Performance: LLM Arena Score vs. Brier Score",
    #   x = "Arena Score (higher is better)",
    #   y = "Brier Score (lower is better)",
    #   color = "Prompt Type",
    #   shape = "Freeze Values"
    # ) +
    theme_minimal() +
    coord_cartesian( # clip = "off",
      xlim = c(NA, 1625),
      ylim = c(0, 0.25)
    ) +
    theme(
      axis.title.x = element_blank(),
      axis.title.y = element_blank()
    )

  print(cor.test(leaderboard$arena_score, leaderboard$overall_score))

  return(p)
}

p <- leaderboard_arena_graphs(human_leaderboard)

ggsave("arena_v_overall.png", p, units = c("px"), width = 1900, height = 1479, bg = "white")

# Import training compute guesses
tc <- read.csv("training_compute_epoch.csv")

leaderboard_compute_graphs <- function(leaderboard) {
  leaderboard <- leaderboard %>%
    select(model, overall_score, arena_score, prompt, freeze_values, news, pretty_name) %>%
    mutate(tc = tc[tc$model == model, ]$tc)
  leaderboard <- leaderboard %>%
    filter(prompt == "scratchpad") %>%
    filter(freeze_values == TRUE) %>%
    filter(news == FALSE)

  # Fit a linear model to find the equation of the smooth line
  lm_fit <- lm(overall_score ~ log(tc), data = leaderboard)
  print(summary(lm_fit))

  # Extract coefficients
  intercept <- coef(lm_fit)[1]
  slope <- coef(lm_fit)[2]

  # Calculate the x-coordinate where y = 0.091
  y_intercept <- supers_brier
  x_intersect <- exp((y_intercept - intercept) / slope)

  # Bootstrap to calculate confidence intervals for the intersection
  set.seed(123) # For reproducibility
  n_bootstraps <- 1000
  bootstrap_intersects <- replicate(n_bootstraps, {
    # Resample the data with replacement
    sample_data <- leaderboard[sample(1:nrow(leaderboard), size = nrow(leaderboard), replace = TRUE), ]

    # Fit the linear model to the resampled data
    lm_boot <- lm(overall_score ~ log(tc), data = sample_data)

    # Extract coefficients
    intercept_boot <- coef(lm_boot)[1]
    slope_boot <- coef(lm_boot)[2]

    # Calculate the x-coordinate of the intersection
    exp((y_intercept - intercept_boot) / slope_boot)
  })

  # Compute the 95% confidence interval for the intersections
  ci_lower <- quantile(bootstrap_intersects, 0.025)
  ci_upper <- quantile(bootstrap_intersects, 0.975)
  print(ci_lower)
  print(ci_upper)

  label_data <- data.frame(
    x = x_intersect,
    y = y_intercept,
    label = "Superforecasters"
  )
  print(label_data)

  p <- ggplot(leaderboard, aes(y = overall_score, x = tc)) +
    geom_point(size = 3, color = "#F8766D") + # Adjust point size
    stat_function(
      fun = function(x) intercept + slope * log(x), # Define the abline as a function
      color = "gray", linetype = "dashed"
    ) + # Linear fit line
    geom_text_repel(aes(label = pretty_name),
      size = 3,
      max.overlaps = Inf,
      hjust = -0.1, # Optional adjustment
      vjust = -1.5 # Optional adjustment
    ) +
    geom_hline(yintercept = supers_brier, linetype = "dotted", color = "blue", size = 1) + # Horizontal line
    geom_point(aes(x = x_intersect, y = y_intercept), color = "red", size = 3) + # Intersection point +
    geom_errorbarh(
      aes(
        xmin = ci_lower,
        xmax = ci_upper,
        y = y_intercept
      ),
      color = "red", height = 0.01, size = 1, alpha = 0.03
    ) +
    # annotate("text", x = x_intersect, y = y_intercept,
    #          label = "Superforecasters",
    #          hjust = 1, vjust = -0.5, color = "red", size = 3) + # Annotate the intersection point
    geom_text_repel(
      data = label_data,
      aes(x = x, y = y, label = label),
      color = "red",
      segment.color = "black",
      size = 3,
      hjust = -0.1, # Optional adjustment
      vjust = -1.5 # Optional adjustment
    ) +
    labs(
      # title = "Model Performance: Estimated Training Compute vs.\nOverall Score",
      x = "Estimated Training Compute",
      y = "Overall Score",
      color = "Prompt Type",
      shape = "Freeze Values"
    ) +
    theme_minimal() +
    coord_cartesian(
      clip = "off",
      xlim = c(NA, 1e+28),
      ylim = c(0, 0.25)
    ) +
    scale_x_continuous(
      trans = pseudo_log_trans(base = 10),
      breaks = c(1e+23, 1e+24, 1e+25, 1e+26, 1e+27, 1e+28)
    ) +
    theme(
      axis.title.x = element_blank(),
      axis.title.y = element_blank()
    )
  print(cor.test(log(leaderboard$tc), leaderboard$overall_score))
  return(p)
}

p <- leaderboard_compute_graphs(human_leaderboard)

ggsave("tc_v_overall.png", p, units = c("px"), width = 1900, height = 1479, bg = "white")
