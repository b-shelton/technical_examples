library(shiny)
library(ggplot2)
library(scales)

shinyServer(function(input, output){

  output$pdf_report <- downloadHandler(
    filename = "AHP_Report.html",
    content = function(file) {
      tempReport <- file.path(tempdir(), "report.Rmd")
      file.copy("report.Rmd", tempReport, overwrite = TRUE)

      params <- list(n = input$critera_text)

      rmarkdown::render(tempReport, output_file = file,
                        params  = params,
                        envir = new.env(parent = globalenv()))
    }
  )



  criteria_names <- reactive({
    y <- as.numeric(input$criteria_count)
    n1 <- strsplit(input$critera_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Criteria ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]
    n1 <- n1
  })

  options_names <- reactive({
    y <- as.numeric(input$options_count)
    n1 <- strsplit(input$options_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Option ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]
    n1 <- n1
  })

  #########################################
  #
  # UI Titles
  #
  #########################################

  output$modelName <- renderUI({
    h1(input$model_name)
  })

  output$c1m_name <- renderUI({
    h3(paste0(substr(criteria_names()[1], 13, nchar(criteria_names()[1])), ": Comparison of Alternatives"))
  })

  output$c2m_name <- renderUI({
    h3(paste0(substr(criteria_names()[2], 13, nchar(criteria_names()[2])), ": Comparison of Alternatives"))
  })

  output$c3m_name <- renderUI({
    if(as.numeric(input$criteria_count) > 2) {
      h3(paste0(substr(criteria_names()[3], 13, nchar(criteria_names()[3])), ": Comparison of Alternatives"))
    } else {
      h3("No 3rd Criteria")
    }
  })

  output$c4m_name <- renderUI({
    if(as.numeric(input$criteria_count) > 3) {
      h3(paste0(substr(criteria_names()[4], 13, nchar(criteria_names()[4])), ": Comparison of Alternatives"))
    } else {
      h3("No 4th Criteria")
    }
  })

  output$c5m_name <- renderUI({
    if(as.numeric(input$criteria_count) > 4) {
      h3(paste0(substr(criteria_names()[5], 13, nchar(criteria_names()[5])), ": Comparison of Alternatives"))
    } else {
      h3("No 5th Criteria")
    }
  })



  #########################################
  #
  # Main Criteria Choices
  #
  #########################################
  output$SelectCriteria1 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][1]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][2]

    selectInput("main_cri1",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria2 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][1]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_cri2",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria3 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][2]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_cri3",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria4 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][1]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_cri4",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria5 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][2]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_cri5",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria6 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][3]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_cri6",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria7 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][1]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_cri7",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria8 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][2]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_cri8",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria9 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][3]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_cri9",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })

  output$SelectCriteria10 <- renderUI({
    criteria1 <- strsplit(input$critera_text, ", ")[[1]][4]
    criteria2 <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_cri10",
                label = paste0("How much more important is ", criteria1, " than ", criteria2, "?"),
                choices = c("9 (Extremely more important)",
                            "7 (Much more important)",
                            "5 (Moderately more important)",
                            "3 (Slightly more important)",
                            "1 (Equally as important)",
                            "1/3 (Slightly less important)",
                            "1/5 (Moderately less important)",
                            "1/7 (Much less important)",
                            "1/9 (Extremely less important)"),
               selected = "1 (Equally as important)")

  })


  #########################################
  #
  # Assessment of Alternatives
  #
  #########################################

  output$OptionAssessment1 <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][2]
    criteria <- strsplit(input$critera_text, ", ")[[1]][1]

    selectInput("main_options1",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment2 <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][1]

    selectInput("main_options2",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment3 <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][1]

    selectInput("main_options3",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment4 <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][1]

    selectInput("main_options4",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment5 <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][1]

    selectInput("main_options5",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment6 <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][3]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][1]

    selectInput("main_options6",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment1.a <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][2]
    criteria <- strsplit(input$critera_text, ", ")[[1]][2]

    selectInput("main_options1.a",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment2.a <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][2]

    selectInput("main_options2.a",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment3.a <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][2]

    selectInput("main_options3.a",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment4.a <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][2]

    selectInput("main_options4.a",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment5.a <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][2]

    selectInput("main_options5.a",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment6.a <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][3]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][2]

    selectInput("main_options6.a",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })


  output$OptionAssessment1.b <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][2]
    criteria <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_options1.b",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment2.b <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_options2.b",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment3.b <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_options3.b",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment4.b <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_options4.b",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment5.b <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_options5.b",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment6.b <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][3]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][3]

    selectInput("main_options6.b",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment1.c <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][2]
    criteria <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_options1.c",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment2.c <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_options2.c",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment3.c <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_options3.c",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment4.c <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_options4.c",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment5.c <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_options5.c",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment6.c <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][3]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][4]

    selectInput("main_options6.c",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment1.d <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][2]
    criteria <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_options1.d",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment2.d <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_options2.d",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment3.d <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][3]
    criteria <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_options3.d",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment4.d <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][1]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_options4.d",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment5.d <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][2]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_options5.d",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })

  output$OptionAssessment6.d <- renderUI({
    option1 <- strsplit(input$options_text, ", ")[[1]][3]
    option2 <- strsplit(input$options_text, ", ")[[1]][4]
    criteria <- strsplit(input$critera_text, ", ")[[1]][5]

    selectInput("main_options6.d",
                label = paste0("How much better is ", option1, " than ", option2, " at meeting ", criteria, "?"),
                c("9 (Extremely better)",
                  "7 (Much better)",
                  "5 (Moderately better)",
                  "3 (Slightly better)",
                  "1 (Equally as well)",
                  "1/3 (Slightly worse)",
                  "1/5 (Moderately worse)",
                  "1/7 (Much worse)",
                  "1/9 (Extremely worse)"),
                  selected = "1 (Equally as well)")

  })



  #########################################
  #
  # Main Criteria Matrix
  #
  #########################################

  mainCriteria <- reactive({
    y <- as.numeric(input$criteria_count)
    n1 <- strsplit(input$critera_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Criteria ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    z <- matrix(data = NA, nrow = y, ncol = y, dimnames = list(n1, n1))

    for(i in 1:nrow(z)) {
      z[i, i] <- 1
    }

    z <- z

    if(y == 2) {

      mc1 <- gsub( " .*$", "", input$main_cri1)
      mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
      z[1, 2] <- mc1
      z[2, 1] <- 1/mc1

    } else {

      if(y == 3) {

        mc1 <- gsub( " .*$", "", input$main_cri1)
        mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
        z[1, 2] <- mc1
        z[2, 1] <- 1/mc1

        mc2 <- gsub( " .*$", "", input$main_cri2)
        mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
        z[1, 3] <- mc2
        z[3, 1] <- 1/mc2

        mc3 <- gsub( " .*$", "", input$main_cri3)
        mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
        z[2, 3] <- mc3
        z[3, 2] <- 1/mc3

      } else {

        if(y == 4) {

          mc1 <- gsub( " .*$", "", input$main_cri1)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_cri2)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_cri3)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_cri4)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_cri5)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_cri6)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

        } else {

          mc1 <- gsub( " .*$", "", input$main_cri1)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_cri2)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_cri3)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_cri4)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_cri5)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_cri6)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

          mc7 <- gsub( " .*$", "", input$main_cri7)
          mc7 <- ifelse(nchar(mc7) > 1, 1 / as.numeric(substr(mc7, 3, 3)), as.numeric(mc7))
          z[1, 5] <- mc7
          z[5, 1] <- 1/mc7

          mc8 <- gsub( " .*$", "", input$main_cri8)
          mc8 <- ifelse(nchar(mc8) > 1, 1 / as.numeric(substr(mc8, 3, 3)), as.numeric(mc8))
          z[2, 5] <- mc8
          z[5, 2] <- 1/mc8

          mc9 <- gsub( " .*$", "", input$main_cri9)
          mc9 <- ifelse(nchar(mc9) > 1, 1 / as.numeric(substr(mc9, 3, 3)), as.numeric(mc9))
          z[3, 5] <- mc9
          z[5, 3] <- 1/mc9

          mc10 <- gsub( " .*$", "", input$main_cri10)
          mc10 <- ifelse(nchar(mc10) > 1, 1 / as.numeric(substr(mc10, 3, 3)), as.numeric(mc10))
          z[4, 5] <- mc10
          z[5, 4] <- 1/mc10
        }
      }
    }

    z <- z
  })

  output$mc_matrix <- renderTable({
    if(length(criteria_names()) == as.numeric(input$criteria_count)) {
      z <- mainCriteria()
    } else {
      z <- "Count of criteria and number of criteria names do no equal."
    }
    print(z)
  }, include.rownames = TRUE)


   mcp <- reactive({
    y <- as.numeric(input$criteria_count)
    n1 <- strsplit(input$critera_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Criteria ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    if(length(n1) == y) {
      z <- mainCriteria()
      #nth root product values
      z2 <- apply(z, 1, prod) ^ (1/ncol(z))
      #priorities
      z3 <- z2 / sum(z2)
      z3 <- data.frame(z3)
      colnames(z3) <- "Main Criteria Prioritization Values"
    } else {
      z3 <- "Count of criteria and number of criteria names do no equal."
    }
    z3 <- z3
  })

  output$matrix1 <- renderTable({
    print(mcp())
  }, include.rownames = TRUE)


  #########################################
  #
  # Main Criteria Consistency Ratio
  #
  #########################################

  cr1 <- reactive({
    z <- mainCriteria()
    #nth root product values
    z2 <- apply(z, 1, prod) ^ (1/ncol(z))
    #priorities
    z3 <- z2 / sum(z2)
    #vectors
    zz <- z
    for(i in 1:ncol(z)) {
      zz[, i] <- zz[, i] * z3[i]
    }
    zz <- apply(zz, 1, sum)
    #lambda max
    zz <- cbind(zz, z3)
    zz <- mean(zz[, 1] / zz[, 2])
    #consistency index
    ci <- (zz - ncol(z)) / (ncol(z) - 1)
    #consistency ratio
    if(ncol(z) == 2) {
      x = 0 } else if(ncol(z) == 3) {
        x = .58 } else if(ncol(z) == 4) {
          x = .9 } else if(ncol(z) == 5) {
            x = 1.12 } else {
              x = 1.24 }
    cr <- round(if(ncol(z) == 2) { ci } else { ci / x }, 2)
    cr <- cr
  })

  output$cr_mc1 <- renderText({
    if(length(criteria_names()) == as.numeric(input$criteria_count)) {
      cr <- cr1()
      if(cr < .15) {
        print("")
      } else {
        if(cr < .5) {
          print("Warning: Moderately inconsistent main criteria pair-wise rankings.")
        } else {
          print("WARNING: Highly inconsistent MAIN CRITERIA pair-wise rankings!")
        }
      }
    } else {
      print("")
    }
  })

  output$cr_mc2 <- renderText({
    if(length(criteria_names()) == as.numeric(input$criteria_count)) {
      cr <- cr1()
      cr_explain <- if(cr < .15) {
        "Nicely consistent pair-wise rankings.     "
      } else {
        if(cr < .5) {
          "Warning: Moderately inconsistent pair-wise rankings.    "
        } else {
          "WARNING: Highly inconsistent pair-wise rankings.    "
        }
      }
      print(paste0(cr_explain, "    Consistency Ratio = ", as.character(cr)))
    } else {
      ""
    }
  })


  #########################################
  #
  # Main Criteria Alternatives Assessments
  #
  #########################################


  alternatives_template <- reactive({
    y <- as.numeric(input$options_count)
    n1 <- strsplit(input$options_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Option ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    z <- matrix(data = NA, nrow = y, ncol = y, dimnames = list(n1, n1))

    for(i in 1:nrow(z)) {
      z[i, i] <- 1
    }

    z <- z
  })


  # Option 1 Assessment
  #########################################
  mc_alt1 <- reactive({
    z <- alternatives_template()
    y <- as.numeric(input$options_count)

    if(y == 2) {

      mc1 <- gsub( " .*$", "", input$main_options1)
      mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
      z[1, 2] <- mc1
      z[2, 1] <- 1/mc1

    } else {

      if(y == 3) {

        mc1 <- gsub( " .*$", "", input$main_options1)
        mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
        z[1, 2] <- mc1
        z[2, 1] <- 1/mc1

        mc2 <- gsub( " .*$", "", input$main_options2)
        mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
        z[1, 3] <- mc2
        z[3, 1] <- 1/mc2

        mc3 <- gsub( " .*$", "", input$main_options3)
        mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
        z[2, 3] <- mc3
        z[3, 2] <- 1/mc3

      } else {

        if(y == 4) {

          mc1 <- gsub( " .*$", "", input$main_options1)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

        } else {

          mc1 <- gsub( " .*$", "", input$main_options1)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

          mc7 <- gsub( " .*$", "", input$main_options7)
          mc7 <- ifelse(nchar(mc7) > 1, 1 / as.numeric(substr(mc7, 3, 3)), as.numeric(mc7))
          z[1, 5] <- mc7
          z[5, 1] <- 1/mc7

          mc8 <- gsub( " .*$", "", input$main_options8)
          mc8 <- ifelse(nchar(mc8) > 1, 1 / as.numeric(substr(mc8, 3, 3)), as.numeric(mc8))
          z[2, 5] <- mc8
          z[5, 2] <- 1/mc8

          mc9 <- gsub( " .*$", "", input$main_options9)
          mc9 <- ifelse(nchar(mc9) > 1, 1 / as.numeric(substr(mc9, 3, 3)), as.numeric(mc9))
          z[3, 5] <- mc9
          z[5, 3] <- 1/mc9

          mc10 <- gsub( " .*$", "", input$main_options10)
          mc10 <- ifelse(nchar(mc10) > 1, 1 / as.numeric(substr(mc10, 3, 3)), as.numeric(mc10))
          z[4, 5] <- mc10
          z[5, 4] <- 1/mc10
        }
      }
    }

    z <- z
  })

  #Alternative 1 Matrix Table
  output$alt1 <- renderTable({
    if(length(options_names()) == as.numeric(input$options_count)) {
      print(mc_alt1())
    } else {
      ""
    }
  }, include.rownames = TRUE)

  #Alternative 1 Priorities
  altp1 <- reactive({
    if(length(options_names()) == as.numeric(input$options_count)) {
      z <- mc_alt1()
      #nth root product values
      z2 <- apply(z, 1, prod) ^ (1/ncol(z))
      #priorities
      z3 <- z2 / sum(z2)
      z3 <- data.frame(z3)
      colnames(z3) <- criteria_names()[1]
    } else {
      z3 <- "Count of alternatives and number of alternative names do no equal."
    }
    z3 <- z3
  })

  #Alternative 1 Consistency Ratios
  alt1_cr1 <- reactive({
    z <- mc_alt1()
    #nth root product values
    z2 <- apply(z, 1, prod) ^ (1/ncol(z))
    #priorities
    z3 <- z2 / sum(z2)
    #vectors
    zz <- z
    for(i in 1:ncol(z)) {
      zz[, i] <- zz[, i] * z3[i]
    }
    zz <- apply(zz, 1, sum)
    #lambda max
    zz <- cbind(zz, z3)
    zz <- mean(zz[, 1] / zz[, 2])
    #consistency index
    ci <- (zz - ncol(z)) / (ncol(z) - 1)
    #consistency ratio
    if(ncol(z) == 2) {
      x = 0 } else if(ncol(z) == 3) {
        x = .58 } else if(ncol(z) == 4) {
          x = .9 } else if(ncol(z) == 5) {
            x = 1.12 } else {
              x = 1.24 }
    cr <- round(if(ncol(z) == 2) { ci } else { ci / x }, 2)
    cr <- cr
  })

  output$cr_alt1 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      cr <- alt1_cr1()
      if(cr < .15) {
        print("")
      } else {
        if(cr < .5) {
          print("Warning on the 1st criteria alternative comparisons: Moderately inconsistent pair-wise rankings.")
        } else {
          print("WARNING on the 1st criteria alternative comparisons: Highly inconsistent pair-wise rankings!")
        }
      }
    } else {
      print("")
    }
  })

  output$cr2_alt1 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      cr <- alt1_cr1()
      cr_explain <- if(cr < .15) {
        "Nicely consistent pair-wise rankings.     "
      } else {
        if(cr < .5) {
          "Warning: Moderately inconsistent pair-wise rankings.    "
        } else {
          "WARNING: Highly inconsistent pair-wise rankings.    "
        }
      }
      print(paste0(cr_explain, "    Consistency Ratio = ", as.character(cr)))
    } else {
      ""
    }
  })




  # Option 2 Assessment
  #########################################
  mc_alt2 <- reactive({
    z <- alternatives_template()
    y <- as.numeric(input$options_count)

    if(y == 2) {

      mc1 <- gsub( " .*$", "", input$main_options1.a)
      mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
      z[1, 2] <- mc1
      z[2, 1] <- 1/mc1

    } else {

      if(y == 3) {

        mc1 <- gsub( " .*$", "", input$main_options1.a)
        mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
        z[1, 2] <- mc1
        z[2, 1] <- 1/mc1

        mc2 <- gsub( " .*$", "", input$main_options2.a)
        mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
        z[1, 3] <- mc2
        z[3, 1] <- 1/mc2

        mc3 <- gsub( " .*$", "", input$main_options3.a)
        mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
        z[2, 3] <- mc3
        z[3, 2] <- 1/mc3

      } else {

        if(y == 4) {

          mc1 <- gsub( " .*$", "", input$main_options1.a)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.a)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.a)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.a)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.a)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.a)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

        } else {

          mc1 <- gsub( " .*$", "", input$main_options1.a)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.a)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.a)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.a)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.a)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.a)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

          mc7 <- gsub( " .*$", "", input$main_options7.a)
          mc7 <- ifelse(nchar(mc7) > 1, 1 / as.numeric(substr(mc7, 3, 3)), as.numeric(mc7))
          z[1, 5] <- mc7
          z[5, 1] <- 1/mc7

          mc8 <- gsub( " .*$", "", input$main_options8.a)
          mc8 <- ifelse(nchar(mc8) > 1, 1 / as.numeric(substr(mc8, 3, 3)), as.numeric(mc8))
          z[2, 5] <- mc8
          z[5, 2] <- 1/mc8

          mc9 <- gsub( " .*$", "", input$main_options9.a)
          mc9 <- ifelse(nchar(mc9) > 1, 1 / as.numeric(substr(mc9, 3, 3)), as.numeric(mc9))
          z[3, 5] <- mc9
          z[5, 3] <- 1/mc9

          mc10 <- gsub( " .*$", "", input$main_options10.a)
          mc10 <- ifelse(nchar(mc10) > 1, 1 / as.numeric(substr(mc10, 3, 3)), as.numeric(mc10))
          z[4, 5] <- mc10
          z[5, 4] <- 1/mc10
        }
      }
    }

    z <- z
  })

  output$alt2 <- renderTable({
    y <- as.numeric(input$options_count)
    n1 <- strsplit(input$options_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Option ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    if(length(n1) == y) {
      print(mc_alt2())
    } else {
      ""
    }
  }, include.rownames = TRUE)


  altp2 <- reactive({
    y <- as.numeric(input$options_count)
    n1 <- strsplit(input$options_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Criteria ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    if(length(n1) == y) {
      z <- mc_alt2()
      #nth root product values
      z2 <- apply(z, 1, prod) ^ (1/ncol(z))
      #priorities
      z3 <- z2 / sum(z2)
      z3 <- data.frame(z3)
      colnames(z3) <- criteria_names()[2]
    } else {
      z3 <- "Count of alternatives and number of alternative names do no equal."
    }
    z3 <- z3
  })


  #Alternative 2 Consistency Ratios
  alt2_cr1 <- reactive({
    z <- mc_alt2()
    #nth root product values
    z2 <- apply(z, 1, prod) ^ (1/ncol(z))
    #priorities
    z3 <- z2 / sum(z2)
    #vectors
    zz <- z
    for(i in 1:ncol(z)) {
      zz[, i] <- zz[, i] * z3[i]
    }
    zz <- apply(zz, 1, sum)
    #lambda max
    zz <- cbind(zz, z3)
    zz <- mean(zz[, 1] / zz[, 2])
    #consistency index
    ci <- (zz - ncol(z)) / (ncol(z) - 1)
    #consistency ratio
    if(ncol(z) == 2) {
      x = 0 } else if(ncol(z) == 3) {
        x = .58 } else if(ncol(z) == 4) {
          x = .9 } else if(ncol(z) == 5) {
            x = 1.12 } else {
              x = 1.24 }
    cr <- round(if(ncol(z) == 2) { ci } else { ci / x }, 2)
    cr <- cr
  })

  output$cr_alt2 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      cr <- alt2_cr1()
      if(cr < .15) {
        print("")
      } else {
        if(cr < .5) {
          print("Warning on the 2nd criteria alternative comparisons: Moderately inconsistent pair-wise rankings.")
        } else {
          print("WARNING on the 2nd criteria alternative comparisons: Highly inconsistent pair-wise rankings!")
        }
      }
    } else {
      print("")
    }
  })

  output$cr2_alt2 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      cr <- alt2_cr1()
      cr_explain <- if(cr < .15) {
        "Nicely consistent pair-wise rankings.     "
      } else {
        if(cr < .5) {
          "Warning: Moderately inconsistent pair-wise rankings.    "
        } else {
          "WARNING: Highly inconsistent pair-wise rankings.    "
        }
      }
      print(paste0(cr_explain, "    Consistency Ratio = ", as.character(cr)))
    } else {
      ""
    }
  })


  # Option 3 Assessment
  #########################################
  mc_alt3 <- reactive({
    z <- alternatives_template()
    y <- as.numeric(input$options_count)

    if(y == 2) {

      mc1 <- gsub( " .*$", "", input$main_options1.b)
      mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
      z[1, 2] <- mc1
      z[2, 1] <- 1/mc1

    } else {

      if(y == 3) {

        mc1 <- gsub( " .*$", "", input$main_options1.b)
        mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
        z[1, 2] <- mc1
        z[2, 1] <- 1/mc1

        mc2 <- gsub( " .*$", "", input$main_options2.b)
        mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
        z[1, 3] <- mc2
        z[3, 1] <- 1/mc2

        mc3 <- gsub( " .*$", "", input$main_options3.b)
        mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
        z[2, 3] <- mc3
        z[3, 2] <- 1/mc3

      } else {

        if(y == 4) {

          mc1 <- gsub( " .*$", "", input$main_options1.b)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.b)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.b)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.b)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.b)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.b)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

        } else {

          mc1 <- gsub( " .*$", "", input$main_options1.b)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.b)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.b)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.b)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.b)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.b)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

          mc7 <- gsub( " .*$", "", input$main_options7.b)
          mc7 <- ifelse(nchar(mc7) > 1, 1 / as.numeric(substr(mc7, 3, 3)), as.numeric(mc7))
          z[1, 5] <- mc7
          z[5, 1] <- 1/mc7

          mc8 <- gsub( " .*$", "", input$main_options8.b)
          mc8 <- ifelse(nchar(mc8) > 1, 1 / as.numeric(substr(mc8, 3, 3)), as.numeric(mc8))
          z[2, 5] <- mc8
          z[5, 2] <- 1/mc8

          mc9 <- gsub( " .*$", "", input$main_options9.b)
          mc9 <- ifelse(nchar(mc9) > 1, 1 / as.numeric(substr(mc9, 3, 3)), as.numeric(mc9))
          z[3, 5] <- mc9
          z[5, 3] <- 1/mc9

          mc10 <- gsub( " .*$", "", input$main_options10.b)
          mc10 <- ifelse(nchar(mc10) > 1, 1 / as.numeric(substr(mc10, 3, 3)), as.numeric(mc10))
          z[4, 5] <- mc10
          z[5, 4] <- 1/mc10
        }
      }
    }

    z <- z
  })

  output$alt3 <- renderTable({
    x <- as.numeric(input$criteria_count)
    if(x > 2) {
      y <- as.numeric(input$options_count)
      n1 <- strsplit(input$options_text, ", ")
      for(i in 1:length(n1[[1]])) {
        n1[[1]][i] <- paste0("Option ", i, ": ", n1[[1]][i])
      }
      n1 <- n1[[1]]

      if(length(n1) == y) {
        z <- mc_alt3()
      } else {
        z <- ""
      }
    } else {
      z <- "This AHP model does not contain a 3rd criteria."
    }
    print(z)
  }, include.rownames = TRUE)


  altp3 <- reactive({
    y <- as.numeric(input$options_count)
    n1 <- strsplit(input$options_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Criteria ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    if(length(n1) == y) {
      z <- mc_alt3()
      #nth root product values
      z2 <- apply(z, 1, prod) ^ (1/ncol(z))
      #priorities
      z3 <- z2 / sum(z2)
      z3 <- data.frame(z3)
      colnames(z3) <- criteria_names()[3]
    } else {
      z3 <- "Count of alternatives and number of alternative names do no equal."
    }
    z3 <- z3
  })

  #Alternative 3 Consistency Ratios
  alt3_cr1 <- reactive({
    z <- mc_alt3()
    #nth root product values
    z2 <- apply(z, 1, prod) ^ (1/ncol(z))
    #priorities
    z3 <- z2 / sum(z2)
    #vectors
    zz <- z
    for(i in 1:ncol(z)) {
      zz[, i] <- zz[, i] * z3[i]
    }
    zz <- apply(zz, 1, sum)
    #lambda max
    zz <- cbind(zz, z3)
    zz <- mean(zz[, 1] / zz[, 2])
    #consistency index
    ci <- (zz - ncol(z)) / (ncol(z) - 1)
    #consistency ratio
    if(ncol(z) == 2) {
      x = 0 } else if(ncol(z) == 3) {
        x = .58 } else if(ncol(z) == 4) {
          x = .9 } else if(ncol(z) == 5) {
            x = 1.12 } else {
              x = 1.24 }
    cr <- round(if(ncol(z) == 2) { ci } else { ci / x }, 2)
    cr <- cr
  })

  output$cr_alt3 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      cr <- alt3_cr1()
      if(cr < .15) {
        print("")
      } else {
        if(cr < .5) {
          print("Warning on the 3rd criteria alternative comparisons: Moderately inconsistent pair-wise rankings.")
        } else {
          print("WARNING on the 3rd criteria alternative comparisons: Highly inconsistent pair-wise rankings!")
        }
      }
    } else {
      print("")
    }
  })

  output$cr2_alt3 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      if(length(criteria_names()) > 2) {
        cr <- alt3_cr1()
        cr_explain <- if(cr < .15) {
          "Nicely consistent pair-wise rankings.     "
        } else {
          if(cr < .5) {
            "Warning: Moderately inconsistent pair-wise rankings.    "
          } else {
            "WARNING: Highly inconsistent pair-wise rankings.    "
          }
        }
        print(paste0(cr_explain, "    Consistency Ratio = ", as.character(cr)))
      } else {
        print("")
      }
    } else {
      print("")
    }
  })


  # Option 4 Assessment
  #########################################
  mc_alt4 <- reactive({
    z <- alternatives_template()
    y <- as.numeric(input$options_count)

    if(y == 2) {

      mc1 <- gsub( " .*$", "", input$main_options1.c)
      mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
      z[1, 2] <- mc1
      z[2, 1] <- 1/mc1

    } else {

      if(y == 3) {

        mc1 <- gsub( " .*$", "", input$main_options1.c)
        mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
        z[1, 2] <- mc1
        z[2, 1] <- 1/mc1

        mc2 <- gsub( " .*$", "", input$main_options2.c)
        mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
        z[1, 3] <- mc2
        z[3, 1] <- 1/mc2

        mc3 <- gsub( " .*$", "", input$main_options3.c)
        mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
        z[2, 3] <- mc3
        z[3, 2] <- 1/mc3

      } else {

        if(y == 4) {

          mc1 <- gsub( " .*$", "", input$main_options1.c)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.c)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.c)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.c)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.c)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.c)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

        } else {

          mc1 <- gsub( " .*$", "", input$main_options1.c)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.c)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.c)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.c)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.c)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.c)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

          mc7 <- gsub( " .*$", "", input$main_options7.c)
          mc7 <- ifelse(nchar(mc7) > 1, 1 / as.numeric(substr(mc7, 3, 3)), as.numeric(mc7))
          z[1, 5] <- mc7
          z[5, 1] <- 1/mc7

          mc8 <- gsub( " .*$", "", input$main_options8.c)
          mc8 <- ifelse(nchar(mc8) > 1, 1 / as.numeric(substr(mc8, 3, 3)), as.numeric(mc8))
          z[2, 5] <- mc8
          z[5, 2] <- 1/mc8

          mc9 <- gsub( " .*$", "", input$main_options9.c)
          mc9 <- ifelse(nchar(mc9) > 1, 1 / as.numeric(substr(mc9, 3, 3)), as.numeric(mc9))
          z[3, 5] <- mc9
          z[5, 3] <- 1/mc9

          mc10 <- gsub( " .*$", "", input$main_options10.c)
          mc10 <- ifelse(nchar(mc10) > 1, 1 / as.numeric(substr(mc10, 3, 3)), as.numeric(mc10))
          z[4, 5] <- mc10
          z[5, 4] <- 1/mc10
        }
      }
    }

    z <- z
  })

  output$alt4 <- renderTable({
    x <- as.numeric(input$criteria_count)
    if(x > 3) {
      y <- as.numeric(input$options_count)
      n1 <- strsplit(input$options_text, ", ")
      for(i in 1:length(n1[[1]])) {
        n1[[1]][i] <- paste0("Option ", i, ": ", n1[[1]][i])
      }
      n1 <- n1[[1]]

      if(length(n1) == y) {
        z <- mc_alt4()
      } else {
        z <- ""
      }
    } else {
      z <- "This AHP model does not contain a 4th criteria."
    }
    print(z)
  }, include.rownames = TRUE)


  altp4 <- reactive({
    y <- as.numeric(input$options_count)
    n1 <- strsplit(input$options_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Criteria ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    if(length(n1) == y) {
      z <- mc_alt4()
      #nth root product values
      z2 <- apply(z, 1, prod) ^ (1/ncol(z))
      #priorities
      z3 <- z2 / sum(z2)
      z3 <- data.frame(z3)
      colnames(z3) <- criteria_names()[4]
    } else {
      z3 <- "Count of alternatives and number of alternative names do no equal."
    }
    z3 <- z3
  })

  #Alternative 4 Consistency Ratios
  alt4_cr1 <- reactive({
    z <- mc_alt4()
    #nth root product values
    z2 <- apply(z, 1, prod) ^ (1/ncol(z))
    #priorities
    z3 <- z2 / sum(z2)
    #vectors
    zz <- z
    for(i in 1:ncol(z)) {
      zz[, i] <- zz[, i] * z3[i]
    }
    zz <- apply(zz, 1, sum)
    #lambda max
    zz <- cbind(zz, z3)
    zz <- mean(zz[, 1] / zz[, 2])
    #consistency index
    ci <- (zz - ncol(z)) / (ncol(z) - 1)
    #consistency ratio
    if(ncol(z) == 2) {
      x = 0 } else if(ncol(z) == 3) {
        x = .58 } else if(ncol(z) == 4) {
          x = .9 } else if(ncol(z) == 5) {
            x = 1.12 } else {
              x = 1.24 }
    cr <- round(if(ncol(z) == 2) { ci } else { ci / x }, 2)
    cr <- cr
  })

  output$cr_alt4 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      cr <- alt4_cr1()
      if(cr < .15) {
        print("")
      } else {
        if(cr < .5) {
          print("Warning on the 4th criteria alternative comparisons: Moderately inconsistent pair-wise rankings.")
        } else {
          print("WARNING on the 4th criteria alternative comparisons: Highly inconsistent pair-wise rankings!")
        }
      }
    } else {
      print("")
    }
  })

  output$cr2_alt4 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      if(length(criteria_names()) > 3) {
        cr <- alt4_cr1()
        cr_explain <- if(cr < .15) {
          "Nicely consistent pair-wise rankings.     "
        } else {
          if(cr < .5) {
            "Warning: Moderately inconsistent pair-wise rankings.    "
          } else {
            "WARNING: Highly inconsistent pair-wise rankings.    "
          }
        }
        print(paste0(cr_explain, "    Consistency Ratio = ", as.character(cr)))
      } else {
        print("")
      }
    } else {
      print("")
    }
  })


  # Option 5 Assessment
  #########################################
  mc_alt5 <- reactive({
    z <- alternatives_template()
    y <- as.numeric(input$options_count)

    if(y == 2) {

      mc1 <- gsub( " .*$", "", input$main_options1.d)
      mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
      z[1, 2] <- mc1
      z[2, 1] <- 1/mc1

    } else {

      if(y == 3) {

        mc1 <- gsub( " .*$", "", input$main_options1.d)
        mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
        z[1, 2] <- mc1
        z[2, 1] <- 1/mc1

        mc2 <- gsub( " .*$", "", input$main_options2.d)
        mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
        z[1, 3] <- mc2
        z[3, 1] <- 1/mc2

        mc3 <- gsub( " .*$", "", input$main_options3.d)
        mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
        z[2, 3] <- mc3
        z[3, 2] <- 1/mc3

      } else {

        if(y == 4) {

          mc1 <- gsub( " .*$", "", input$main_options1.d)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.d)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.d)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.d)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.d)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.d)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

        } else {

          mc1 <- gsub( " .*$", "", input$main_options1.d)
          mc1 <- ifelse(nchar(mc1) > 1, 1 / as.numeric(substr(mc1, 3, 3)), as.numeric(mc1))
          z[1, 2] <- mc1
          z[2, 1] <- 1/mc1

          mc2 <- gsub( " .*$", "", input$main_options2.d)
          mc2 <- ifelse(nchar(mc2) > 1, 1 / as.numeric(substr(mc2, 3, 3)), as.numeric(mc2))
          z[1, 3] <- mc2
          z[3, 1] <- 1/mc2

          mc3 <- gsub( " .*$", "", input$main_options3.d)
          mc3 <- ifelse(nchar(mc3) > 1, 1 / as.numeric(substr(mc3, 3, 3)), as.numeric(mc3))
          z[2, 3] <- mc3
          z[3, 2] <- 1/mc3

          mc4 <- gsub( " .*$", "", input$main_options4.d)
          mc4 <- ifelse(nchar(mc4) > 1, 1 / as.numeric(substr(mc4, 3, 3)), as.numeric(mc4))
          z[1, 4] <- mc4
          z[4, 1] <- 1/mc4

          mc5 <- gsub( " .*$", "", input$main_options5.d)
          mc5 <- ifelse(nchar(mc5) > 1, 1 / as.numeric(substr(mc5, 3, 3)), as.numeric(mc5))
          z[2, 4] <- mc5
          z[4, 2] <- 1/mc5

          mc6 <- gsub( " .*$", "", input$main_options6.d)
          mc6 <- ifelse(nchar(mc6) > 1, 1 / as.numeric(substr(mc6, 3, 3)), as.numeric(mc6))
          z[3, 4] <- mc6
          z[4, 3] <- 1/mc6

          mc7 <- gsub( " .*$", "", input$main_options7.d)
          mc7 <- ifelse(nchar(mc7) > 1, 1 / as.numeric(substr(mc7, 3, 3)), as.numeric(mc7))
          z[1, 5] <- mc7
          z[5, 1] <- 1/mc7

          mc8 <- gsub( " .*$", "", input$main_options8.d)
          mc8 <- ifelse(nchar(mc8) > 1, 1 / as.numeric(substr(mc8, 3, 3)), as.numeric(mc8))
          z[2, 5] <- mc8
          z[5, 2] <- 1/mc8

          mc9 <- gsub( " .*$", "", input$main_options9.d)
          mc9 <- ifelse(nchar(mc9) > 1, 1 / as.numeric(substr(mc9, 3, 3)), as.numeric(mc9))
          z[3, 5] <- mc9
          z[5, 3] <- 1/mc9

          mc10 <- gsub( " .*$", "", input$main_options10.d)
          mc10 <- ifelse(nchar(mc10) > 1, 1 / as.numeric(substr(mc10, 3, 3)), as.numeric(mc10))
          z[4, 5] <- mc10
          z[5, 4] <- 1/mc10
        }
      }
    }

    z <- z
  })

  output$alt5 <- renderTable({
    x <- as.numeric(input$criteria_count)
    if(x > 4) {
      y <- as.numeric(input$options_count)
      n1 <- strsplit(input$options_text, ", ")
      for(i in 1:length(n1[[1]])) {
        n1[[1]][i] <- paste0("Option ", i, ": ", n1[[1]][i])
      }
      n1 <- n1[[1]]

      if(length(n1) == y) {
        z <- mc_alt5()
      } else {
        z <- ""
      }
    } else {
      z <- "This AHP model does not contain a 5th criteria."
    }
    print(z)
  }, include.rownames = TRUE)


  altp5 <- reactive({
    y <- as.numeric(input$options_count)
    n1 <- strsplit(input$options_text, ", ")
    for(i in 1:length(n1[[1]])) {
      n1[[1]][i] <- paste0("Criteria ", i, ": ", n1[[1]][i])
    }
    n1 <- n1[[1]]

    if(length(n1) == y) {
      z <- mc_alt5()
      #nth root product values
      z2 <- apply(z, 1, prod) ^ (1/ncol(z))
      #priorities
      z3 <- z2 / sum(z2)
      z3 <- data.frame(z3)
      colnames(z3) <- criteria_names()[5]
    } else {
      z3 <- "Count of alternatives and number of alternative names do no equal."
    }
    z3 <- z3
  })


  #Alternative 5 Consistency Ratios
  alt5_cr1 <- reactive({
    z <- mc_alt5()
    #nth root product values
    z2 <- apply(z, 1, prod) ^ (1/ncol(z))
    #priorities
    z3 <- z2 / sum(z2)
    #vectors
    zz <- z
    for(i in 1:ncol(z)) {
      zz[, i] <- zz[, i] * z3[i]
    }
    zz <- apply(zz, 1, sum)
    #lambda max
    zz <- cbind(zz, z3)
    zz <- mean(zz[, 1] / zz[, 2])
    #consistency index
    ci <- (zz - ncol(z)) / (ncol(z) - 1)
    #consistency ratio
    if(ncol(z) == 2) {
      x = 0 } else if(ncol(z) == 3) {
        x = .58 } else if(ncol(z) == 4) {
          x = .9 } else if(ncol(z) == 5) {
            x = 1.12 } else {
              x = 1.24 }
    cr <- round(if(ncol(z) == 2) { ci } else { ci / x }, 2)
    cr <- cr
  })

  output$cr_alt5 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      if(length(criteria_names()) > 4) {
        cr <- alt5_cr1()
        if(cr < .15) {
          print("")
        } else {
          if(cr < .5) {
            print("Warning on the 5th criteria alternative comparisons: Moderately inconsistent pair-wise rankings.")
          } else {
            print("WARNING on the 5th criteria alternative comparisons: Highly inconsistent pair-wise rankings!")
          }
        }
      } else {
        print("")
      }
    } else {
      print("")
    }
  })

  output$cr2_alt5 <- renderText({
    if(length(options_names()) == as.numeric(input$options_count)) {
      if(length(criteria_names()) > 4) {
        cr <- alt5_cr1()
        cr_explain <- if(cr < .15) {
          "Nicely consistent pair-wise rankings.     "
        } else {
          if(cr < .5) {
            "Warning: Moderately inconsistent pair-wise rankings.    "
          } else {
            "WARNING: Highly inconsistent pair-wise rankings.    "
          }
        }
        print(paste0(cr_explain, "    Consistency Ratio = ", as.character(cr)))
      } else {
        print("")
      }
    } else {
      print("")
    }
  })


  alt_priorities <- reactive({
    if(length(criteria_names()) == as.numeric(input$criteria_count) & length(options_names()) == as.numeric(input$options_count)) {
      if(length(criteria_names()) == 2) {
        x <- cbind(altp1(), altp2())
      } else {
        if(length(criteria_names()) == 3) {
          x <- cbind(altp1(), altp2(), altp3())
        } else {
          if(length(criteria_names()) == 4) {
            x <- cbind(altp1(), altp2(), altp3(), altp4())
          } else {
            if(length(criteria_names()) == 5) {
              x <- cbind(altp1(), altp2(), altp3(), altp4(), altp5())
            }
          }
        }
      }
    } else {
      if(length(criteria_names()) != as.numeric(input$criteria_count) & length(options_names()) != as.numeric(input$options_count)) {
        x <- "Neither the names of criteria or alternatives match their respective counts."
      } else {
        if(length(criteria_names()) != as.numeric(input$criteria_count) & length(options_names()) == as.numeric(input$options_count)) {
          x <- "Count of criteria and number of criteria names do no equal."
        } else {
          if(length(criteria_names()) == as.numeric(input$criteria_count) & length(options_names()) != as.numeric(input$options_count)) {
            x <- "Count of alternatives and number of alternative names do no equal."
          }
        }
      }
    }
    x <- x
  })

  output$alt_priorities <- renderTable({
    print(alt_priorities())
  }, include.rownames = TRUE)


  decision_outcome <- reactive({
    x <- alt_priorities()
    for(i in 1:ncol(x)) {
      x[, i] <- x[, i] * mcp()[i, ]
    }
    x <- data.frame(apply(x, 1, sum))
    colnames(x) <- "rank"
    x <- x
  })

  output$decision_outcome <- renderPlot({

    if(length(criteria_names()) != as.numeric(input$criteria_count) |
       length(options_names()) != as.numeric(input$options_count)) {

      t <- data.frame(x = 1, y = 1)
      u <- ggplot(data = t, aes(x = x, y = y)) +
        geom_point() +
        theme_bw() +
        ggtitle("Criteria and/or alternative counts and name lengths do not match.") +
        labs(x = "", y = "") +
        theme(plot.title = element_text(size = 18, face = "bold"),
              axis.title.y = element_text(size = 22, margin = margin(0, 20, 0 , 0)),
              axis.title.x = element_text(size = 22, margin = margin(20, 0, 0 , 0)))

    } else {

      t <- decision_outcome()
      u <- ggplot(t, aes(x = rownames(t), y = round(rank, 2))) +
        geom_bar(stat = "identity", fill = "#FF9999", colour = "black") +
        theme_bw() +
        ggtitle("") +
        labs(x = "Alternatives", y = "Prioritization") +
        theme(axis.title.y = element_text(size = 22, margin = margin(0, 20, 0 , 0)),
              axis.title.x = element_text(size = 22, margin = margin(20, 0, 0 , 0)),
              axis.text = element_text(size = 18),
              axis.text.y = element_blank()) +
        geom_text(aes(label = percent(round(rank, 2))), vjust = -.5, size = 6) +
        scale_y_continuous(limit = c(0, 1))

    }

    print(u)

  })


  plotOut = function() {
    if(length(criteria_names()) != as.numeric(input$criteria_count) |
       length(options_names()) != as.numeric(input$options_count)) {

      t <- data.frame(x = 1, y = 1)
      u <- ggplot(data = t, aes(x = x, y = y)) +
        geom_point() +
        theme_bw() +
        ggtitle("Criteria and/or alternative counts and name lengths do not match.") +
        labs(x = "", y = "") +
        theme(plot.title = element_text(size = 18, face = "bold"),
              axis.title.y = element_text(size = 22, margin = margin(0, 20, 0 , 0)),
              axis.title.x = element_text(size = 22, margin = margin(20, 0, 0 , 0)))
    } else {
      t <- decision_outcome()
      u <- ggplot(t, aes(x = rownames(t), y = round(rank, 2))) +
        geom_bar(stat = "identity", fill = "#FF9999", colour = "black") +
        theme_bw() +
        ggtitle("") +
        labs(x = "Alternatives", y = "Prioritization") +
        theme(axis.title.y = element_text(size = 22, margin = margin(0, 20, 0 , 0)),
              axis.title.x = element_text(size = 22, margin = margin(20, 0, 0 , 0)),
              axis.text = element_text(size = 18),
              axis.text.y = element_blank()) +
        geom_text(aes(label = percent(round(rank, 2))), vjust = -.5, size = 6) +
        scale_y_continuous(limit = c(0, 1))
    }
    print(u)
  }


  output$foo = downloadHandler(
    filename = 'AHP_output.png',
    content = function(file) {
      device <- function(..., width, height) {
        grDevices::png(..., width = width, height = height,
                       res = 300, units = "in")
      }
      ggsave(file, plot = plotOut(), device = device)
    })

})
