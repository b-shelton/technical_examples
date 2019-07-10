#
# This is the user-interface definition of a Shiny web application. You can
# run the application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

# this Shiny apps site: https://bshelton141.shinyapps.io/Miles_per_Gallon_Comparison/

library(shiny)

shinyUI(fluidPage(
  titlePanel("Miles per Gallon Comparison from the 'mtcars' Data Set"),
  sidebarLayout(
    sidebarPanel(h3("Choose Car Feature Parameters"),
                 sliderInput("slidebar1", 
                             "Select Horsepower Range", 
                             min(mtcars$hp), 
                             max(mtcars$hp), 
                             c(min(mtcars$hp), 
                               max(mtcars$hp))),
                 selectInput("select1",
                             "Select Number of Cylinders",
                             c("All", sort(mtcars[which(!duplicated(mtcars$cyl)), ]$cyl)),
                             multiple = FALSE)),
    mainPanel(plotOutput("plot1"))
  )
))
