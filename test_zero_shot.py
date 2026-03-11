from transformers import pipeline

print("Loading model...")
classifier = pipeline(
    "zero-shot-classification", 
    model="MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli-ling-wanli"
)

# A varied sample of entities and their glosses
samples = [
    {
        "name": "Gunther Gebel-Williams",
        "gloss": "a famous German-American animal trainer and circus performer"
    },
    {
        "name": "The United Nations",
        "gloss": "an intergovernmental organization whose stated purposes are to maintain international peace and security"
    },
    {
        "name": "Mount Everest",
        "gloss": "Earth's highest mountain above sea level, located in the Himalayas"
    },
    {
        "name": "Water",
        "gloss": "a transparent, tasteless, odorless, and nearly colorless chemical substance"
    },
    {
        "name": "The Magna Carta",
        "gloss": "a royal charter of rights agreed to by King John of England in 1215"
    },
    {
        "name": "The 2008 Financial Crisis",
        "gloss": "a severe worldwide economic crisis that occurred in the early 21st century"
    }
]

# Map the long NLI hypotheses back to your clean, short labels
dimension_map = {
    "having material substance and mass": "physical",
    "a discrete, countable, individuated whole": "bounded",
    "a spatial region that can be occupied": "locative",
    "biologically alive and capable of autonomous movement": "animate",
    "possessing subjective experience, perception, or feeling": "sentient",
    "having the capacity for intention, goal-directed behavior, and decision-making": "volitional",
    "a socially constructed role, formal organizational framework, or legal reality": "institutional",
    "a plurality composed of recognizable, distinct member entities": "collective",
    "possessing an intrinsic, designed purpose or intended use": "telic",
    "bearing, conveying, or representing decodable meaning or informational content": "symbolic",
    "defining a position or value within an ordered system or magnitude dimension": "scalar",
    "defined by or bounded to a time interval or temporal horizon": "temporal"
}

# Extract just the long descriptions for the classifier to use
dimensions = list(dimension_map.keys())

print("\n--- Running Batch Zero-Shot Classification ---\n")

for sample in samples:
    # THE FIX: Force the model to look at the real-world referent
    premise = f"In the real world, this is {sample['gloss']}."
    print(f"Entity: {sample['name']}")
    
    results = classifier(premise, dimensions, multi_label=True)
    
    # Print the top 4 dimensions using the clean short labels
    for label, score in zip(results['labels'][:4], results['scores'][:4]):
        short_label = dimension_map[label]
        print(f"  {score:.4f} : {short_label}")
    print("-" * 50)