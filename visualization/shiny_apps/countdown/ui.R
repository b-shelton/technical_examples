shinyUI(
  fluidPage(
    hr(),
    actionButton('start','Start'),
    actionButton('stop','Stop'),
    actionButton('reset','Reset'),
    numericInput('seconds','Seconds:',value=10,min=0,max=99999,step=1),
    textOutput('timeleft')

  )
)
