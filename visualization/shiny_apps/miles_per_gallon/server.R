#
# This is the server logic of a Shiny web application. You can run the 
# application by clicking 'Run App' above.
#
# Find out more about building applications with Shiny here:
# 
#    http://shiny.rstudio.com/
#

# this Shiny apps site: https://bshelton141.shinyapps.io/Miles_per_Gallon_Comparison/


library(shiny)

# Define server logic required to draw a histogram
shinyServer(function(input, output) {
  
  output$plot1 <- renderPlot({
    x1 <- subset(mtcars, hp >= input$slidebar1[1] & hp <= input$slidebar1[2])
    if(input$select1 == "All") {
      x2 <- x1
    } else {
      x2 <- subset(x1, cyl == input$select1)
    }
    x2$transmission <- ifelse(x2$am == 0, "automatic", "manual")
    
    library(ggplot2)
    ggplot(data = x2, aes(x = reorder(rownames(x2), -mpg), y = mpg, fill = as.factor(cyl))) + 
      geom_bar(stat = "identity") +
      labs(title = "Miles per Gallon by Make and Model",
           x = "Make and Model") +
      scale_fill_discrete(name = "Cylinders") +
      theme(axis.text.x = element_text(angle = 90, hjust = 1))
  })
  
