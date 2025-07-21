# ### Visaisation: 
# 
# ### risk over Time:

# In[51]:


get_ipython().system('pip install matplotlib seaborn')


# In[52]:


import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10, 6))
sns.lineplot(data=combined_df, x='year', y='traffic_risk_score', marker='o')
plt.title('Traffic Risk Score Over Time')
plt.ylabel('Risk Score (Normalized)')
plt.xlabel('Year')
plt.grid(True)
plt.tight_layout()
plt.show()


# ### Rainfall vs Traffic Risk score

# In[53]:


plt.figure(figsize=(10, 6))
sns.scatterplot(data=combined_df, x='rainfall_mm', y='traffic_risk_score', hue='year', palette='viridis', s=100)
plt.title('Rainfall vs. Traffic Risk Score')
plt.xlabel('Rainfall (mm)')
plt.ylabel('Traffic Risk Score')
plt.grid(True)
plt.tight_layout()
plt.show()


# ### Casulties per year

# In[ ]:


plt.figure(figsize=(12, 7))
combined_df.set_index('year')[['Death', 'Severe Injury', 'Slight Injury']].plot(
    kind='bar', stacked=True, colormap='Set2', figsize=(12, 7)
)
plt.title('Casualties by Type Over the Years')
plt.ylabel('Number of People')
plt.xlabel('Year')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()


# ### Composite RIsk Score Over Time: 

# In[54]:


import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10, 5))
sns.lineplot(data=combined_df, x="year", y="traffic_risk_score", marker="o", label="Traffic Risk")
sns.lineplot(data=combined_df, x="year", y="rainfall_score", marker="o", label="Rainfall Score")
sns.lineplot(data=combined_df, x="year", y="composite_risk_score", marker="o", label="Composite Risk")

plt.title("Composite and Component Risk Scores Over Time")
plt.xlabel("Year")
plt.ylabel("Normalized Risk Score")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()


# In[ ]:




