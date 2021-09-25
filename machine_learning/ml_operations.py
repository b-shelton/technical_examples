'''
This file provides manual explanations of common machine learning functions
'''
import matplotlib.pyplot as plt

# # Root Mean Squared Error
# ### Purpose: Gauge the ability of a regression-based model
# ---------------------------------------------------------------------------------------------------
y = [150, 100, 24, 66, 42, 55, 36, 125, 133]
y_hat1 = [160, 99, 36, 50, 30, 57, 33, 110, 155]
y_hat2 = [180, 50, 99, 35, 56, 63, 30, 105, 160]

# Calculate the RMSE of y_hat1
(np.sum([(i-j)**2 for i, j in zip(y,y_hat1)])/len(y)) ** .5

def regression_outcomes(predictions):
    rmse = (np.sum([(i-j)**2 for i, j in zip(y,predictions)])/len(y)) ** .5
    plt.scatter(y, predictions)
    plt.title(f'Predictions with RMSE: {np.round(rmse, 3)}')
    plt.show()

regression_outcomes(y_hat1)
regression_outcomes(y_hat2)


# # Softmax Probability
# ### Purpose: Categorical activiation function to turn a set of numbers into probabilities for
# ### categorical predictions. The sum of softmax always totals 1.
# ---------------------------------------------------------------------------------------------------
import math
output_scores = [14, 12, 8]
softmax_denom = np.sum([math.exp(j) for j in output_scores])
softmax_probs = [math.exp(i) / softmax_denom for i in output_scores]
print(f'The softmax probability of scores {output_scores} is {softmax_probs}.') 
np.sum(softmax_probs)
