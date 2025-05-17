"""
Skill recommendation module for AI job displacement risk analysis.
Provides recommended skills to develop based on job type and risk assessment.
"""

# Dictionary mapping job categories to recommended skills
SKILL_RECOMMENDATIONS = {
    'technical': {
        'core': [
            "Advanced problem-solving",
            "Systems thinking",
            "High-level design",
            "Cross-functional collaboration",
            "Complex project management"
        ],
        'emerging': [
            "AI/ML ethics",
            "Human-AI collaboration",
            "AI systems supervision",
            "Novel use-case development",
            "Algorithm auditing"
        ],
        'soft': [
            "Creative thinking",
            "Interpersonal communication",
            "Leadership and mentoring",
            "Stakeholder management",
            "Strategic planning"
        ]
    },
    'computing': {
        'core': [
            "Advanced software architecture",
            "Systems integration",
            "Complex algorithm optimization",
            "Security/privacy engineering",
            "Distributed systems design"
        ],
        'emerging': [
            "Prompt engineering",
            "AI model fine-tuning",
            "Human-in-the-loop systems",
            "Neural network architecture",
            "Machine learning operations"
        ],
        'soft': [
            "Technical leadership",
            "Cross-team collaboration",
            "Technical communications",
            "Innovation management",
            "Product vision development"
        ]
    },
    'data_science': {
        'core': [
            "Advanced statistical modeling",
            "Causal inference",
            "Research design",
            "Domain expertise development",
            "Experiment design"
        ],
        'emerging': [
            "AI safety research",
            "Explainable AI development",
            "AI systems oversight",
            "Novel algorithm design",
            "Ethical framework development"
        ],
        'soft': [
            "Data storytelling",
            "Multi-stakeholder communication",
            "Research leadership",
            "Strategic decision making",
            "Complex problem framing"
        ]
    },
    'administrative': {
        'core': [
            "Process optimization",
            "System integration knowledge",
            "Advanced data analysis",
            "Project management",
            "Risk assessment"
        ],
        'emerging': [
            "AI tool operation",
            "AI output verification",
            "Workflow automation design",
            "Digital transformation management",
            "AI-assisted decision making"
        ],
        'soft': [
            "Complex stakeholder management",
            "Change management",
            "Crisis management",
            "Strategic thinking",
            "Team leadership"
        ]
    },
    'management': {
        'core': [
            "Strategic planning",
            "Change management",
            "Organizational development",
            "Executive decision making",
            "Technology integration planning"
        ],
        'emerging': [
            "AI strategy development",
            "Human-AI team management",
            "Technological transformation leadership",
            "Ethical AI governance",
            "AI risk management"
        ],
        'soft': [
            "Inspirational leadership",
            "Complex stakeholder communication",
            "Adaptability and resilience building",
            "Organizational culture development",
            "Vision setting"
        ]
    },
    'healthcare': {
        'core': [
            "Advanced patient care",
            "Specialized clinical expertise",
            "Medical research literacy",
            "Integrated care management",
            "Health systems knowledge"
        ],
        'emerging': [
            "AI-assisted diagnostics",
            "Health tech integration",
            "AI output verification",
            "Digital health services",
            "AI-human collaborative care"
        ],
        'soft': [
            "Advanced empathy and communication",
            "Crisis management",
            "Interdisciplinary collaboration",
            "Patient advocacy",
            "Healthcare leadership"
        ]
    },
    'education': {
        'core': [
            "Curriculum development",
            "Personalized learning design",
            "Educational assessment expertise",
            "Learning psychology",
            "Subject matter mastery"
        ],
        'emerging': [
            "AI-enhanced teaching methods",
            "Educational technology integration",
            "Adaptive learning system design",
            "Human-AI collaborative teaching",
            "Digital pedagogy"
        ],
        'soft': [
            "Emotional intelligence",
            "Mentorship and coaching",
            "Cultural competence",
            "Creative engagement",
            "Ethical leadership"
        ]
    },
    'service': {
        'core': [
            "Complex customer needs assessment",
            "Problem resolution",
            "Service design",
            "Specialized product knowledge",
            "Quality assurance"
        ],
        'emerging': [
            "AI-augmented customer service",
            "Digital service integration",
            "Automated system supervision",
            "Experience personalization",
            "Technology-assisted service delivery"
        ],
        'soft': [
            "Advanced empathy",
            "Cultural awareness",
            "Situation handling",
            "Relationship building",
            "Communication excellence"
        ]
    },
    'creative': {
        'core': [
            "Advanced design thinking",
            "Creative innovation",
            "Multi-media integration",
            "Concept development",
            "Artistic mastery"
        ],
        'emerging': [
            "Human-AI collaborative creation",
            "AI tool utilization for creatives",
            "Novel creative workflows",
            "Prompt design for generative systems",
            "AI output enhancement and direction"
        ],
        'soft': [
            "Design communication",
            "Interdisciplinary collaboration",
            "Critical thinking",
            "Cultural trend analysis",
            "Creative leadership"
        ]
    },
    'legal': {
        'core': [
            "Complex legal analysis",
            "Case strategy development",
            "Legal research expertise",
            "Legal writing mastery",
            "Specialized domain knowledge"
        ],
        'emerging': [
            "AI-assisted legal research",
            "Legal tech integration",
            "Automated document analysis oversight",
            "AI ethics and law",
            "Technology regulation expertise"
        ],
        'soft': [
            "Client relationship management",
            "Negotiation excellence",
            "Public speaking",
            "Ethical judgment",
            "Professional leadership"
        ]
    },
    # Food service specific skills
    'culinary': {
        'core': [
            "Advanced culinary techniques",
            "Menu design and innovation",
            "Ingredient knowledge and sourcing",
            "Flavor profile development",
            "Food safety and quality control"
        ],
        'emerging': [
            "Automated kitchen equipment operation",
            "Recipe optimization software",
            "Digital inventory management",
            "Food tech integration",
            "Sustainable food systems knowledge"
        ],
        'soft': [
            "Team coordination",
            "Creative problem-solving",
            "Sensory evaluation",
            "Customer experience design",
            "Adaptability under pressure"
        ]
    },
    # Transportation specific skills
    'transportation': {
        'core': [
            "Complex logistics planning",
            "Transportation systems knowledge",
            "Safety protocol expertise",
            "Route optimization",
            "Emergency response skills"
        ],
        'emerging': [
            "Autonomous vehicle supervision",
            "Smart transportation systems",
            "Advanced telemetry analytics",
            "Electric vehicle technology",
            "Transportation technology integration"
        ],
        'soft': [
            "Situational awareness",
            "Customer service excellence",
            "Communication clarity",
            "Decision making under pressure",
            "Adaptability to changing conditions"
        ]
    },
    # Manufacturing specific skills
    'manufacturing': {
        'core': [
            "Advanced production oversight",
            "Quality control systems",
            "Process optimization",
            "Supply chain management",
            "Technical troubleshooting"
        ],
        'emerging': [
            "Robotic systems supervision",
            "Smart factory technologies",
            "IoT and sensor network management",
            "Digital twin technology",
            "Predictive maintenance systems"
        ],
        'soft': [
            "Cross-functional coordination",
            "Continuous improvement mindset",
            "Problem-solving under constraints",
            "Team leadership",
            "Safety culture development"
        ]
    },
    # Retail specific skills
    'retail': {
        'core': [
            "Complex customer needs assessment",
            "Visual merchandising",
            "Inventory management",
            "Product knowledge specialization",
            "Sales strategy development"
        ],
        'emerging': [
            "Omnichannel retail management",
            "E-commerce integration",
            "Digital payment systems",
            "Automated retail technologies",
            "Customer data analytics"
        ],
        'soft': [
            "Persuasive communication",
            "Emotional intelligence",
            "Conflict resolution",
            "Cultural sensitivity",
            "Adaptability to changing trends"
        ]
    },
    # Financial specific skills
    'financial': {
        'core': [
            "Complex financial analysis",
            "Risk assessment",
            "Regulatory compliance",
            "Strategic financial planning",
            "Portfolio management"
        ],
        'emerging': [
            "Fintech integration",
            "Algorithmic trading oversight",
            "Blockchain and cryptocurrency",
            "Automated financial systems",
            "AI-assisted financial planning"
        ],
        'soft': [
            "Ethical decision making",
            "Client relationship management",
            "Financial communication",
            "Critical thinking",
            "Complex problem solving"
        ]
    }
}

# Default recommendations for categories not specifically mapped
DEFAULT_RECOMMENDATIONS = {
    'core': [
        "Complex problem solving",
        "Critical thinking",
        "Systems thinking",
        "Interdisciplinary knowledge",
        "Professional expertise development"
    ],
    'emerging': [
        "AI literacy",
        "Technology adaptation",
        "Human-AI collaboration",
        "Digital tool mastery",
        "New technology integration"
    ],
    'soft': [
        "Adaptability",
        "Creativity",
        "Communication excellence",
        "Emotional intelligence",
        "Leadership and influence"
    ]
}

def get_skill_recommendations(job_category, risk_level, job_title=''):
    """
    Get skill recommendations based on job category, risk level and job title.
    
    Args:
        job_category (str): The job category (e.g., 'technical', 'healthcare')
        risk_level (str): Risk level (e.g., 'Low', 'Moderate', 'High', 'Very High')
        job_title (str): The job title for more specific recommendations
    
    Returns:
        dict: Dictionary with skill recommendations by category
    """
    # Map specific job titles to specialized categories
    job_title_map = {
        'cook': 'culinary',
        'chef': 'culinary',
        'sous chef': 'culinary',
        'pastry chef': 'culinary',
        'head chef': 'culinary',
        'culinary': 'culinary',
        
        'driver': 'transportation',
        'truck driver': 'transportation',
        'delivery driver': 'transportation',
        'pilot': 'transportation',
        'chauffeur': 'transportation',
        'transportation': 'transportation',
        
        'factory worker': 'manufacturing',
        'assembly worker': 'manufacturing',
        'production worker': 'manufacturing',
        'machinist': 'manufacturing',
        'manufacturing': 'manufacturing',
        
        'retail': 'retail',
        'sales associate': 'retail',
        'cashier': 'retail',
        'store manager': 'retail',
        'merchandiser': 'retail',
        
        'accountant': 'financial',
        'banker': 'financial',
        'financial advisor': 'financial',
        'finance': 'financial',
        'investment': 'financial',
        'financial': 'financial'
    }
    
    # Check if job title matches any specialized category
    specialized_category = None
    job_title_lower = job_title.lower()
    
    # First try exact matches
    for title, category in job_title_map.items():
        if job_title_lower == title:
            specialized_category = category
            break
    
    # If no exact match, try partial matches
    if not specialized_category:
        for title, category in job_title_map.items():
            if title in job_title_lower:
                specialized_category = category
                break
    
    # Use specialized category if found, otherwise use job_category
    final_category = specialized_category or job_category
    
    # Get category recommendations, defaulting if not available
    category_recs = SKILL_RECOMMENDATIONS.get(final_category, DEFAULT_RECOMMENDATIONS)
    
    # Customize message based on risk level
    if risk_level in ["High", "Very High"]:
        urgency = "high priority"
        approach = f"These skills are crucial for maintaining employability as AI significantly impacts the {final_category.replace('_', ' ')} field."
    elif risk_level == "Moderate":
        urgency = "recommended"
        approach = f"Developing these skills will help you adapt as AI gradually transforms aspects of your role in {final_category.replace('_', ' ')}."
    else:  # Low
        urgency = "beneficial"
        approach = f"These skills will complement your expertise and help you leverage AI as it evolves in the {final_category.replace('_', ' ')} field."
    
    result = {
        'core_skills': category_recs['core'],
        'emerging_skills': category_recs['emerging'],
        'soft_skills': category_recs['soft'],
        'urgency': urgency,
        'approach': approach,
        'category': final_category.replace('_', ' ')
    }
    
    return result

def get_adaptation_strategies(job_category, risk_level, job_title=''):
    """
    Get specific adaptation strategies based on job category and risk level.
    
    Args:
        job_category (str): The job category
        risk_level (str): Risk level
        job_title (str): Optional job title for more specific strategies
    
    Returns:
        list: List of adaptation strategies
    """
    # Map specific job titles to specialized categories
    job_title_map = {
        'cook': 'culinary',
        'chef': 'culinary',
        'sous chef': 'culinary',
        'pastry chef': 'culinary',
        'head chef': 'culinary',
        'culinary': 'culinary',
        
        'driver': 'transportation',
        'truck driver': 'transportation',
        'delivery driver': 'transportation',
        'pilot': 'transportation',
        'chauffeur': 'transportation',
        'transportation': 'transportation',
        
        'factory worker': 'manufacturing',
        'assembly worker': 'manufacturing',
        'production worker': 'manufacturing',
        'machinist': 'manufacturing',
        'manufacturing': 'manufacturing',
        
        'retail': 'retail',
        'sales associate': 'retail',
        'cashier': 'retail',
        'store manager': 'retail',
        'merchandiser': 'retail',
        
        'accountant': 'financial',
        'banker': 'financial',
        'financial advisor': 'financial',
        'finance': 'financial',
        'investment': 'financial',
        'financial': 'financial'
    }
    
    # Check if job title matches any specialized category
    specialized_category = None
    if job_title:
        job_title_lower = job_title.lower()
        
        # First try exact matches
        for title, category in job_title_map.items():
            if job_title_lower == title:
                specialized_category = category
                break
        
        # If no exact match, try partial matches
        if not specialized_category:
            for title, category in job_title_map.items():
                if title in job_title_lower:
                    specialized_category = category
                    break
    
    # Use specialized category if found, otherwise use job_category
    final_category = specialized_category or job_category
    
    # Common strategies for all jobs
    common_strategies = [
        "Stay informed about AI developments in your field",
        "Pursue continuous learning and professional development",
        "Build a strong professional network"
    ]
    
    # Risk-level specific strategies
    if risk_level == "Very High":
        risk_strategies = [
            "Consider transition to adjacent roles that require more human judgment",
            "Develop expertise in areas where AI currently struggles",
            "Learn to supervise and verify AI systems in your domain",
            "Consider upskilling or career pivoting to lower-risk domains"
        ]
    elif risk_level == "High":
        risk_strategies = [
            "Focus on developing complex skills that complement AI capabilities",
            "Position yourself at the human-AI interface in your field",
            "Specialize in areas requiring complex judgment and creativity",
            "Develop the ability to work alongside AI tools effectively"
        ]
    elif risk_level == "Moderate":
        risk_strategies = [
            "Focus on skills that combine technical expertise with human elements",
            "Learn to effectively use AI tools to enhance your productivity",
            "Develop your unique expertise that differentiates you from AI",
            "Stay adaptable as your role evolves with technology"
        ]
    else:  # Low
        risk_strategies = [
            "Leverage AI tools to enhance your effectiveness",
            "Focus on developing the human elements of your work",
            "Position yourself as an expert who can integrate AI appropriately",
            "Continue deepening your domain expertise"
        ]
    
    # Category-specific strategies
    category_strategies = {
        'technical': [
            "Focus on high-level system design rather than implementation details",
            "Develop expertise in ethical technology development",
            "Learn to design AI-resistant systems and workflows"
        ],
        'computing': [
            "Shift focus from coding to system architecture and integration",
            "Develop expertise in emerging technologies beyond current AI capabilities",
            "Build skills in AI oversight and governance"
        ],
        'data_science': [
            "Focus on research question formulation and experimental design",
            "Develop domain expertise alongside technical skills",
            "Build skills in AI ethics, safety, and governance"
        ],
        'administrative': [
            "Develop skills in complex coordination and exception handling",
            "Focus on stakeholder management and complex communication",
            "Learn to design and supervise automated workflows"
        ],
        'management': [
            "Develop expertise in human-AI team management",
            "Build skills in ethical decision-making and governance",
            "Focus on strategic leadership and organizational development"
        ],
        'healthcare': [
            "Focus on complex care coordination and patient relationships",
            "Develop expertise in areas requiring human judgment and empathy",
            "Learn to effectively integrate AI diagnostic tools into practice"
        ],
        'education': [
            "Focus on mentorship, motivation, and personalized guidance",
            "Develop expertise in areas requiring human judgment and creativity",
            "Learn to effectively integrate AI tools into educational practices"
        ],
        'service': [
            "Focus on complex customer interactions requiring empathy",
            "Develop expertise in exception handling and complex problem-solving",
            "Build skills in supervising and managing automated systems"
        ],
        'culinary': [
            "Develop signature dishes and unique culinary styles that showcase creativity",
            "Focus on complex flavor development and food design that AI can't easily replicate",
            "Learn to integrate and supervise automated kitchen equipment",
            "Build skills in food experience design and personalized customer interaction"
        ],
        'transportation': [
            "Develop expertise in complex transportation scenarios that automated systems struggle with",
            "Learn to supervise and monitor autonomous vehicle systems",
            "Focus on specialized transport niches requiring complex judgment",
            "Combine driving skills with customer service and human interaction"
        ],
        'manufacturing': [
            "Develop skills in robotic system supervision and quality control",
            "Learn advanced troubleshooting for complex manufacturing systems",
            "Specialize in customized or precision manufacturing requiring human judgment",
            "Build expertise in production systems integration and optimization"
        ],
        'retail': [
            "Focus on personalized customer experiences and relationship building",
            "Develop expertise in product curation and specialized knowledge",
            "Learn to integrate digital and physical retail experiences",
            "Build skills in experiential retail that automation can't easily replicate"
        ],
        'financial': [
            "Develop expertise in complex financial planning requiring human judgment",
            "Focus on relationship-based financial services with high trust requirements",
            "Learn to effectively use and validate AI-based financial analysis tools",
            "Build specialized knowledge in emerging financial domains"
        ]
    }
    
    # Job-specific strategies
    job_specific_strategies = {
        'software engineer': [
            "Shift focus from implementation to architectural design and system planning",
            "Develop expertise in governing and managing AI-based development systems",
            "Focus on complex problem domains where AI tools are still limited",
            "Build skills in human-AI collaboration and prompt engineering"
        ],
        'data scientist': [
            "Focus on business problem formulation and translating needs into models",
            "Develop expertise in ethical AI and responsible model development",
            "Specialize in experimental design and causal inference beyond correlation",
            "Build skills in communicating insights and strategic recommendations"
        ],
        'cook': [
            "Focus on creative cuisine development and signature techniques",
            "Develop expertise in food presentation and dining experiences",
            "Learn to effectively integrate and oversee kitchen automation",
            "Build skills in menu design and culinary innovation"
        ],
        'chef': [
            "Develop unique culinary vision and creative direction skills",
            "Focus on team leadership and kitchen management",
            "Build expertise in complex flavor profiles and innovative techniques",
            "Learn to integrate traditional methods with modern food technology"
        ],
        'driver': [
            "Develop expertise in handling exceptional or complex driving scenarios",
            "Focus on customer service aspects of transportation roles",
            "Learn to operate and supervise semi-autonomous vehicles",
            "Build specialized skills for transport niches resistant to full automation"
        ],
        'teacher': [
            "Focus on mentorship, motivation, and personalized guidance",
            "Develop expertise in designing meaningful learning experiences",
            "Learn to effectively integrate AI educational tools to enhance teaching",
            "Build skills in areas requiring emotional intelligence and creativity"
        ]
    }
    
    # Check for specific job title match
    job_specific = []
    if job_title:
        job_title_lower = job_title.lower()
        for title, strategies in job_specific_strategies.items():
            if title in job_title_lower:
                job_specific = strategies
                break
    
    # Combine strategies - prioritize job-specific strategies if available
    if job_specific:
        return common_strategies + risk_strategies + job_specific
    else:
        specific_strategies = category_strategies.get(final_category, [])
        return common_strategies + risk_strategies + specific_strategies