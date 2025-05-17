"""
Simplified job comparison module with direct data for reliable comparisons.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# Direct data for job comparisons to avoid web scraping and processing issues
JOB_DATA = {
    # Technical/IT Roles
    'Software Engineer': {
        'job_title': 'Software Engineer',
        'year_1_risk': 10.0,
        'year_5_risk': 30.0,
        'risk_level': 'Low',
        'job_category': 'technical',
        'risk_factors': {
            'Task Automation': 35,
            'AI Code Generation': 50,
            'Requirements Analysis': 25,
            'System Design': 15,
            'Domain Knowledge': 10
        }
    },
    'Data Scientist': {
        'job_title': 'Data Scientist',
        'year_1_risk': 5.0,
        'year_5_risk': 20.0,
        'risk_level': 'Low',
        'job_category': 'technical',
        'risk_factors': {
            'Model Training': 30,
            'Data Preprocessing': 45,
            'Feature Engineering': 35,
            'Domain Interpretation': 10,
            'Business Strategy': 5
        }
    },
    'Web Developer': {
        'job_title': 'Web Developer',
        'year_1_risk': 15.0,
        'year_5_risk': 35.0,
        'risk_level': 'Moderate',
        'job_category': 'technical',
        'risk_factors': {
            'Template Usage': 55,
            'Front-end Design': 40,
            'Back-end Logic': 30,
            'Cross-browser Testing': 25,
            'User Experience': 20
        }
    },
    'DevOps Engineer': {
        'job_title': 'DevOps Engineer',
        'year_1_risk': 12.0,
        'year_5_risk': 28.0,
        'risk_level': 'Low',
        'job_category': 'technical',
        'risk_factors': {
            'Infrastructure as Code': 40,
            'CI/CD Pipeline': 35,
            'Cloud Management': 30,
            'Security Implementation': 20,
            'Incident Response': 15
        }
    },
    'Database Administrator': {
        'job_title': 'Database Administrator',
        'year_1_risk': 20.0,
        'year_5_risk': 45.0,
        'risk_level': 'Moderate',
        'job_category': 'technical',
        'risk_factors': {
            'Query Optimization': 50,
            'Backup Automation': 60,
            'Database Design': 35,
            'Security Management': 30,
            'Disaster Recovery': 25
        }
    },
    'IT Support Technician': {
        'job_title': 'IT Support Technician',
        'year_1_risk': 28.0,
        'year_5_risk': 60.0,
        'risk_level': 'High',
        'job_category': 'technical',
        'risk_factors': {
            'Password Resets': 75,
            'Basic Troubleshooting': 65,
            'Software Installation': 60,
            'Hardware Diagnostics': 45,
            'Network Configuration': 40
        }
    },
    'Cybersecurity Analyst': {
        'job_title': 'Cybersecurity Analyst',
        'year_1_risk': 7.0,
        'year_5_risk': 22.0,
        'risk_level': 'Low',
        'job_category': 'technical',
        'risk_factors': {
            'Vulnerability Scanning': 45,
            'Security Monitoring': 35,
            'Threat Analysis': 20,
            'Incident Response': 15,
            'Security Strategy': 10
        }
    },
    'AI Engineer': {
        'job_title': 'AI Engineer',
        'year_1_risk': 3.0,
        'year_5_risk': 15.0,
        'risk_level': 'Low',
        'job_category': 'technical',
        'risk_factors': {
            'Model Training': 25,
            'Algorithm Design': 20,
            'Research Application': 15,
            'System Architecture': 10,
            'Domain Expertise': 5
        }
    },
    
    # Management Roles
    'Project Manager': {
        'job_title': 'Project Manager',
        'year_1_risk': 35.0,
        'year_5_risk': 55.0,
        'risk_level': 'Moderate',
        'job_category': 'management',
        'risk_factors': {
            'Scheduling Automation': 60,
            'Resource Allocation': 45,
            'Status Reporting': 70,
            'Risk Assessment': 30,
            'Stakeholder Management': 20
        }
    },
    'Product Manager': {
        'job_title': 'Product Manager',
        'year_1_risk': 25.0,
        'year_5_risk': 45.0,
        'risk_level': 'Moderate',
        'job_category': 'management',
        'risk_factors': {
            'Market Analysis': 40,
            'Feature Prioritization': 35,
            'Roadmap Planning': 30,
            'User Research': 25,
            'Strategic Decision-Making': 15
        }
    },
    'Operations Manager': {
        'job_title': 'Operations Manager',
        'year_1_risk': 30.0,
        'year_5_risk': 50.0,
        'risk_level': 'Moderate',
        'job_category': 'management',
        'risk_factors': {
            'Process Optimization': 55,
            'Resource Scheduling': 50,
            'Performance Tracking': 45,
            'Quality Assurance': 40,
            'Team Leadership': 25
        }
    },
    'Human Resources Manager': {
        'job_title': 'Human Resources Manager',
        'year_1_risk': 20.0,
        'year_5_risk': 40.0,
        'risk_level': 'Moderate',
        'job_category': 'management',
        'risk_factors': {
            'Benefits Administration': 60,
            'Employee Records': 55,
            'Policy Enforcement': 45,
            'Talent Acquisition': 30,
            'Conflict Resolution': 15
        }
    },
    'Chief Executive Officer': {
        'job_title': 'Chief Executive Officer',
        'year_1_risk': 8.0,
        'year_5_risk': 22.0,
        'risk_level': 'Low',
        'job_category': 'management',
        'risk_factors': {
            'Market Analysis': 35,
            'Financial Review': 30,
            'Strategic Planning': 20,
            'Leadership Vision': 10,
            'Stakeholder Relations': 5
        }
    },
    
    # Healthcare Roles
    'Nurse': {
        'job_title': 'Nurse',
        'year_1_risk': 5.0,
        'year_5_risk': 15.0,
        'risk_level': 'Low',
        'job_category': 'healthcare',
        'risk_factors': {
            'Patient Monitoring': 30,
            'Record Keeping': 50,
            'Medication Management': 35,
            'Patient Care': 5,
            'Emotional Support': 5
        }
    },
    'Physician': {
        'job_title': 'Physician',
        'year_1_risk': 4.0,
        'year_5_risk': 13.0,
        'risk_level': 'Low',
        'job_category': 'healthcare',
        'risk_factors': {
            'Diagnostic Analysis': 25,
            'Treatment Planning': 20,
            'Clinical Documentation': 40,
            'Patient Interaction': 5,
            'Medical Decision Making': 5
        }
    },
    'Medical Technician': {
        'job_title': 'Medical Technician',
        'year_1_risk': 15.0,
        'year_5_risk': 38.0,
        'risk_level': 'Moderate',
        'job_category': 'healthcare',
        'risk_factors': {
            'Sample Analysis': 45,
            'Equipment Operation': 40,
            'Data Recording': 50,
            'Protocol Following': 35,
            'Quality Control': 30
        }
    },
    'Pharmacist': {
        'job_title': 'Pharmacist',
        'year_1_risk': 12.0,
        'year_5_risk': 30.0,
        'risk_level': 'Low',
        'job_category': 'healthcare',
        'risk_factors': {
            'Prescription Verification': 40,
            'Drug Interaction Checking': 35,
            'Inventory Management': 55,
            'Patient Consultation': 20,
            'Compounding': 25
        }
    },
    'Physical Therapist': {
        'job_title': 'Physical Therapist',
        'year_1_risk': 6.0,
        'year_5_risk': 16.0,
        'risk_level': 'Low',
        'job_category': 'healthcare',
        'risk_factors': {
            'Exercise Prescription': 25,
            'Treatment Documentation': 45,
            'Progress Assessment': 30,
            'Manual Therapy': 10,
            'Patient Education': 15
        }
    },
    'Radiologist': {
        'job_title': 'Radiologist',
        'year_1_risk': 18.0,
        'year_5_risk': 42.0,
        'risk_level': 'Moderate',
        'job_category': 'healthcare',
        'risk_factors': {
            'Image Interpretation': 55,
            'Pattern Recognition': 50,
            'Report Generation': 60,
            'Diagnostic Reasoning': 30,
            'Clinical Correlation': 25
        }
    },
    
    # Education Roles
    'Teacher': {
        'job_title': 'Teacher',
        'year_1_risk': 10.0,
        'year_5_risk': 25.0,
        'risk_level': 'Low',
        'job_category': 'education',
        'risk_factors': {
            'Content Delivery': 40,
            'Grading': 60,
            'Lesson Planning': 30,
            'Student Engagement': 10,
            'Emotional Support': 5
        }
    },
    'College Professor': {
        'job_title': 'College Professor',
        'year_1_risk': 8.0,
        'year_5_risk': 22.0,
        'risk_level': 'Low',
        'job_category': 'education',
        'risk_factors': {
            'Research Publication': 30,
            'Lecture Preparation': 35,
            'Student Assessment': 45,
            'Academic Advising': 20,
            'Subject Expertise': 10
        }
    },
    'School Administrator': {
        'job_title': 'School Administrator',
        'year_1_risk': 15.0,
        'year_5_risk': 30.0,
        'risk_level': 'Low',
        'job_category': 'education',
        'risk_factors': {
            'Budget Management': 40,
            'Schedule Creation': 45,
            'Policy Implementation': 35,
            'Staff Evaluation': 25,
            'Community Relations': 15
        }
    },
    'School Counselor': {
        'job_title': 'School Counselor',
        'year_1_risk': 7.0,
        'year_5_risk': 18.0,
        'risk_level': 'Low',
        'job_category': 'education',
        'risk_factors': {
            'Student Assessment': 30,
            'Record Keeping': 45,
            'Academic Planning': 35,
            'Crisis Intervention': 10,
            'Emotional Support': 5
        }
    },
    
    # Service Industry Roles
    'Cook': {
        'job_title': 'Cook',
        'year_1_risk': 25.0,
        'year_5_risk': 45.0,
        'risk_level': 'Moderate',
        'job_category': 'service',
        'risk_factors': {
            'Routine Preparation': 60,
            'Recipe Following': 50,
            'Food Assembly': 45,
            'Menu Creation': 20,
            'Taste Evaluation': 10
        }
    },
    'Waiter/Waitress': {
        'job_title': 'Waiter/Waitress',
        'year_1_risk': 40.0,
        'year_5_risk': 65.0,
        'risk_level': 'High',
        'job_category': 'service',
        'risk_factors': {
            'Order Taking': 75,
            'Food Delivery': 60,
            'Payment Processing': 70,
            'Customer Interaction': 40,
            'Special Requests': 35
        }
    },
    'Bartender': {
        'job_title': 'Bartender',
        'year_1_risk': 30.0,
        'year_5_risk': 50.0,
        'risk_level': 'Moderate',
        'job_category': 'service',
        'risk_factors': {
            'Drink Preparation': 55,
            'Recipe Memorization': 45,
            'Cash Handling': 60,
            'Customer Engagement': 25,
            'Atmosphere Creation': 15
        }
    },
    'Hotel Receptionist': {
        'job_title': 'Hotel Receptionist',
        'year_1_risk': 45.0,
        'year_5_risk': 70.0,
        'risk_level': 'High',
        'job_category': 'service',
        'risk_factors': {
            'Check-in Processing': 75,
            'Reservation Management': 70,
            'Payment Handling': 65,
            'Guest Inquiry Response': 50,
            'Problem Resolution': 45
        }
    },
    'Retail Salesperson': {
        'job_title': 'Retail Salesperson',
        'year_1_risk': 38.0,
        'year_5_risk': 60.0,
        'risk_level': 'High',
        'job_category': 'service',
        'risk_factors': {
            'Inventory Management': 65,
            'Transaction Processing': 70,
            'Product Knowledge': 50,
            'Customer Assistance': 45,
            'Sales Techniques': 40
        }
    },
    'Customer Service Representative': {
        'job_title': 'Customer Service Representative',
        'year_1_risk': 48.0,
        'year_5_risk': 75.0,
        'risk_level': 'High',
        'job_category': 'service',
        'risk_factors': {
            'Inquiry Response': 80,
            'Complaint Processing': 70,
            'Account Management': 65,
            'Policy Explanation': 60,
            'Issue Resolution': 55
        }
    },
    
    # Finance Roles
    'Accountant': {
        'job_title': 'Accountant',
        'year_1_risk': 30.0,
        'year_5_risk': 65.0,
        'risk_level': 'High',
        'job_category': 'finance',
        'risk_factors': {
            'Transaction Processing': 75,
            'Financial Reporting': 65,
            'Tax Preparation': 60,
            'Audit Procedures': 50,
            'Financial Analysis': 40
        }
    },
    'Financial Analyst': {
        'job_title': 'Financial Analyst',
        'year_1_risk': 22.0,
        'year_5_risk': 45.0,
        'risk_level': 'Moderate',
        'job_category': 'finance',
        'risk_factors': {
            'Data Collection': 60,
            'Trend Analysis': 45,
            'Report Generation': 55,
            'Investment Research': 35,
            'Strategic Recommendation': 25
        }
    },
    'Investment Banker': {
        'job_title': 'Investment Banker',
        'year_1_risk': 15.0,
        'year_5_risk': 35.0,
        'risk_level': 'Moderate',
        'job_category': 'finance',
        'risk_factors': {
            'Financial Modeling': 50,
            'Market Analysis': 40,
            'Deal Structuring': 30,
            'Client Relationships': 15,
            'Strategic Negotiation': 10
        }
    },
    'Insurance Underwriter': {
        'job_title': 'Insurance Underwriter',
        'year_1_risk': 40.0,
        'year_5_risk': 75.0,
        'risk_level': 'High',
        'job_category': 'finance',
        'risk_factors': {
            'Risk Assessment': 80,
            'Policy Evaluation': 75,
            'Rate Calculation': 70,
            'Application Review': 65,
            'Decision Documentation': 60
        }
    },
    'Bank Teller': {
        'job_title': 'Bank Teller',
        'year_1_risk': 55.0,
        'year_5_risk': 85.0,
        'risk_level': 'Very High',
        'job_category': 'finance',
        'risk_factors': {
            'Cash Handling': 90,
            'Transaction Processing': 85,
            'Account Verification': 80,
            'Customer Service': 60,
            'Security Procedures': 55
        }
    },
    
    # Legal Roles
    'Lawyer': {
        'job_title': 'Lawyer',
        'year_1_risk': 15.0,
        'year_5_risk': 35.0,
        'risk_level': 'Moderate',
        'job_category': 'legal',
        'risk_factors': {
            'Document Review': 60,
            'Legal Research': 45,
            'Contract Analysis': 40,
            'Case Strategy': 20,
            'Client Representation': 10
        }
    },
    'Paralegal': {
        'job_title': 'Paralegal',
        'year_1_risk': 35.0,
        'year_5_risk': 65.0,
        'risk_level': 'High',
        'job_category': 'legal',
        'risk_factors': {
            'Document Preparation': 75,
            'Legal Research': 65,
            'Case Organization': 60,
            'Client Communication': 50,
            'Filing Procedures': 70
        }
    },
    'Court Reporter': {
        'job_title': 'Court Reporter',
        'year_1_risk': 45.0,
        'year_5_risk': 80.0,
        'risk_level': 'Very High',
        'job_category': 'legal',
        'risk_factors': {
            'Transcription': 90,
            'Verbatim Recording': 85,
            'Document Formatting': 70,
            'Legal Terminology': 60,
            'Court Procedures': 55
        }
    },
    'Judge': {
        'job_title': 'Judge',
        'year_1_risk': 8.0,
        'year_5_risk': 25.0,
        'risk_level': 'Low',
        'job_category': 'legal',
        'risk_factors': {
            'Legal Research': 40,
            'Precedent Analysis': 35,
            'Decision Documentation': 30,
            'Case Evaluation': 15,
            'Ethical Judgment': 5
        }
    },
    'Legal Secretary': {
        'job_title': 'Legal Secretary',
        'year_1_risk': 42.0,
        'year_5_risk': 72.0,
        'risk_level': 'High',
        'job_category': 'legal',
        'risk_factors': {
            'Document Preparation': 80,
            'Scheduling': 75,
            'Filing': 70,
            'Client Communication': 65,
            'Legal Correspondence': 60
        }
    },
    'Claims Adjuster': {
        'job_title': 'Claims Adjuster',
        'year_1_risk': 35.0,
        'year_5_risk': 65.0,
        'risk_level': 'High',
        'job_category': 'legal',
        'risk_factors': {
            'Document Review': 70,
            'Damage Assessment': 65,
            'Policy Interpretation': 60,
            'Settlement Calculation': 55,
            'Investigation': 45
        }
    },
    
    # Restaurant Industry Roles
    'Chef': {
        'job_title': 'Chef',
        'year_1_risk': 18.0,
        'year_5_risk': 35.0,
        'risk_level': 'Moderate',
        'job_category': 'restaurant',
        'risk_factors': {
            'Menu Planning': 40,
            'Food Preparation': 35,
            'Recipe Creation': 25,
            'Kitchen Management': 20,
            'Culinary Innovation': 15
        }
    },
    'Restaurant Manager': {
        'job_title': 'Restaurant Manager',
        'year_1_risk': 25.0,
        'year_5_risk': 45.0,
        'risk_level': 'Moderate',
        'job_category': 'restaurant',
        'risk_factors': {
            'Inventory Management': 55,
            'Staff Scheduling': 50,
            'Financial Reporting': 45,
            'Customer Service': 30,
            'Conflict Resolution': 25
        }
    },
    'Hostess': {
        'job_title': 'Hostess',
        'year_1_risk': 50.0,
        'year_5_risk': 75.0,
        'risk_level': 'High',
        'job_category': 'restaurant',
        'risk_factors': {
            'Reservation Management': 80,
            'Seating Allocation': 75,
            'Customer Greeting': 65,
            'Wait Time Estimation': 60,
            'Table Status Tracking': 55
        }
    },
    'Dishwasher': {
        'job_title': 'Dishwasher',
        'year_1_risk': 60.0,
        'year_5_risk': 85.0,
        'risk_level': 'Very High',
        'job_category': 'restaurant',
        'risk_factors': {
            'Repetitive Tasks': 90,
            'Dish Sorting': 85,
            'Cleaning Procedures': 80,
            'Equipment Operation': 75,
            'Kitchen Organization': 65
        }
    },
    'Line Cook': {
        'job_title': 'Line Cook',
        'year_1_risk': 35.0,
        'year_5_risk': 55.0,
        'risk_level': 'Moderate',
        'job_category': 'restaurant',
        'risk_factors': {
            'Food Preparation': 65,
            'Recipe Following': 60,
            'Order Timing': 50,
            'Quality Control': 45,
            'Equipment Usage': 40
        }
    },
    'Food Delivery Driver': {
        'job_title': 'Food Delivery Driver',
        'year_1_risk': 28.0,
        'year_5_risk': 68.0,
        'risk_level': 'High',
        'job_category': 'restaurant',
        'risk_factors': {
            'Navigation': 75,
            'Order Handling': 65,
            'Customer Interaction': 55,
            'Time Management': 50,
            'Vehicle Operation': 45
        }
    },
    
    # Emergency Services Roles
    'Police Officer': {
        'job_title': 'Police Officer',
        'year_1_risk': 8.0,
        'year_5_risk': 20.0,
        'risk_level': 'Low',
        'job_category': 'emergency',
        'risk_factors': {
            'Report Filing': 45,
            'Routine Patrols': 35,
            'Traffic Management': 30,
            'Crisis Response': 10,
            'Community Engagement': 5
        }
    },
    'Firefighter': {
        'job_title': 'Firefighter',
        'year_1_risk': 5.0,
        'year_5_risk': 15.0,
        'risk_level': 'Low',
        'job_category': 'emergency',
        'risk_factors': {
            'Equipment Maintenance': 30,
            'Building Inspection': 25,
            'Report Documentation': 40,
            'Emergency Response': 5,
            'Rescue Operations': 5
        }
    },
    'EMT/Paramedic': {
        'job_title': 'EMT/Paramedic',
        'year_1_risk': 7.0,
        'year_5_risk': 18.0,
        'risk_level': 'Low',
        'job_category': 'emergency',
        'risk_factors': {
            'Patient Assessment': 30,
            'Medical Documentation': 45,
            'Treatment Protocols': 35,
            'Emergency Response': 10,
            'Critical Decision Making': 5
        }
    },
    'Emergency Room Nurse': {
        'job_title': 'Emergency Room Nurse',
        'year_1_risk': 8.0,
        'year_5_risk': 20.0,
        'risk_level': 'Low',
        'job_category': 'emergency',
        'risk_factors': {
            'Patient Triage': 35,
            'Medical Documentation': 45,
            'Treatment Administration': 30,
            'Critical Care': 10,
            'Crisis Management': 5
        }
    },
    'Air Traffic Controller': {
        'job_title': 'Air Traffic Controller',
        'year_1_risk': 15.0,
        'year_5_risk': 35.0,
        'risk_level': 'Moderate',
        'job_category': 'emergency',
        'risk_factors': {
            'Flight Tracking': 45,
            'Communications': 40,
            'Coordination': 35,
            'Crisis Response': 15,
            'Decision Making': 10
        }
    },
    'Dispatcher': {
        'job_title': 'Dispatcher',
        'year_1_risk': 25.0,
        'year_5_risk': 60.0,
        'risk_level': 'High',
        'job_category': 'emergency',
        'risk_factors': {
            'Call Receiving': 70,
            'Information Recording': 65,
            'Resource Allocation': 55,
            'Emergency Prioritization': 40,
            'Multi-tasking': 35
        }
    },
    
    # Creative Roles
    'Graphic Designer': {
        'job_title': 'Graphic Designer',
        'year_1_risk': 15.0,
        'year_5_risk': 40.0,
        'risk_level': 'Moderate',
        'job_category': 'creative',
        'risk_factors': {
            'Template Usage': 60,
            'Basic Layout': 50,
            'Image Editing': 45,
            'Typography': 35,
            'Creative Concept': 20
        }
    },
    'Writer': {
        'job_title': 'Writer',
        'year_1_risk': 18.0,
        'year_5_risk': 45.0,
        'risk_level': 'Moderate',
        'job_category': 'creative',
        'risk_factors': {
            'Content Generation': 55,
            'Research Compilation': 50,
            'Grammar Checking': 60,
            'Creative Storytelling': 25,
            'Voice Development': 20
        }
    },
    'Marketing Specialist': {
        'job_title': 'Marketing Specialist',
        'year_1_risk': 20.0,
        'year_5_risk': 50.0,
        'risk_level': 'Moderate',
        'job_category': 'creative',
        'risk_factors': {
            'Content Creation': 60,
            'Data Analysis': 55,
            'Campaign Scheduling': 50,
            'Strategy Development': 35,
            'Brand Positioning': 30
        }
    },
    'Photographer': {
        'job_title': 'Photographer',
        'year_1_risk': 12.0,
        'year_5_risk': 35.0,
        'risk_level': 'Moderate',
        'job_category': 'creative',
        'risk_factors': {
            'Technical Editing': 50,
            'Basic Composition': 40,
            'Lighting Setup': 35,
            'Creative Vision': 20,
            'Subject Direction': 15
        }
    },
    'Film Director': {
        'job_title': 'Film Director',
        'year_1_risk': 8.0,
        'year_5_risk': 25.0,
        'risk_level': 'Low',
        'job_category': 'creative',
        'risk_factors': {
            'Shot Selection': 35,
            'Technical Coordination': 30,
            'Scene Blocking': 25,
            'Narrative Control': 15,
            'Artistic Vision': 10
        }
    },
    
    # Transportation Roles
    'Truck Driver': {
        'job_title': 'Truck Driver',
        'year_1_risk': 25.0,
        'year_5_risk': 65.0,
        'risk_level': 'High',
        'job_category': 'transportation',
        'risk_factors': {
            'Route Navigation': 70,
            'Vehicle Operation': 60,
            'Loading/Unloading': 55,
            'Safety Compliance': 50,
            'Time Management': 45
        }
    },
    'Taxi Driver': {
        'job_title': 'Taxi Driver',
        'year_1_risk': 40.0,
        'year_5_risk': 80.0,
        'risk_level': 'Very High',
        'job_category': 'transportation',
        'risk_factors': {
            'Route Selection': 85,
            'Navigation': 80,
            'Customer Service': 60,
            'Vehicle Operation': 75,
            'Payment Processing': 70
        }
    },
    'Delivery Driver': {
        'job_title': 'Delivery Driver',
        'year_1_risk': 30.0,
        'year_5_risk': 70.0,
        'risk_level': 'High',
        'job_category': 'transportation',
        'risk_factors': {
            'Route Planning': 75,
            'Package Handling': 65,
            'Scanning/Tracking': 70,
            'Customer Interaction': 55,
            'Vehicle Operation': 60
        }
    },
    'Airline Pilot': {
        'job_title': 'Airline Pilot',
        'year_1_risk': 10.0,
        'year_5_risk': 25.0,
        'risk_level': 'Low',
        'job_category': 'transportation',
        'risk_factors': {
            'Automated Navigation': 40,
            'System Monitoring': 35,
            'Takeoff/Landing': 20,
            'Emergency Response': 15,
            'Decision Making': 10
        }
    },
    
    # Manufacturing/Construction Roles
    'Factory Worker': {
        'job_title': 'Factory Worker',
        'year_1_risk': 45.0,
        'year_5_risk': 80.0,
        'risk_level': 'Very High',
        'job_category': 'manufacturing',
        'risk_factors': {
            'Repetitive Tasks': 90,
            'Assembly Line Work': 85,
            'Quality Checking': 75,
            'Machine Operation': 70,
            'Production Pacing': 65
        }
    },
    'Construction Worker': {
        'job_title': 'Construction Worker',
        'year_1_risk': 30.0,
        'year_5_risk': 60.0,
        'risk_level': 'High',
        'job_category': 'manufacturing',
        'risk_factors': {
            'Material Handling': 65,
            'Basic Assembly': 60,
            'Tool Operation': 55,
            'Site Safety': 45,
            'Physical Endurance': 40
        }
    },
    'Electrician': {
        'job_title': 'Electrician',
        'year_1_risk': 15.0,
        'year_5_risk': 35.0,
        'risk_level': 'Moderate',
        'job_category': 'manufacturing',
        'risk_factors': {
            'Wiring Installation': 45,
            'System Testing': 40,
            'Troubleshooting': 35,
            'Safety Protocol': 30,
            'Blueprint Reading': 25
        }
    },
    'Architect': {
        'job_title': 'Architect',
        'year_1_risk': 12.0,
        'year_5_risk': 30.0,
        'risk_level': 'Low',
        'job_category': 'manufacturing',
        'risk_factors': {
            'CAD Drafting': 45,
            'Standard Compliance': 40,
            'Technical Documentation': 35,
            'Design Concept': 20,
            'Client Consultation': 15
        }
    }
}

# Categories for easier selection
JOB_CATEGORIES = {
    'technical': ['Software Engineer', 'Data Scientist', 'Web Developer', 'DevOps Engineer', 
                 'Database Administrator', 'IT Support Technician', 'Cybersecurity Analyst', 'AI Engineer'],
    'management': ['Project Manager', 'Product Manager', 'Operations Manager', 'Human Resources Manager', 'Chief Executive Officer'],
    'healthcare': ['Nurse', 'Physician', 'Medical Technician', 'Pharmacist', 'Physical Therapist', 'Radiologist'],
    'education': ['Teacher', 'College Professor', 'School Administrator', 'School Counselor'],
    'service': ['Cook', 'Waiter/Waitress', 'Bartender', 'Hotel Receptionist', 'Retail Salesperson', 'Customer Service Representative'],
    'restaurant': ['Chef', 'Restaurant Manager', 'Hostess', 'Dishwasher', 'Line Cook', 'Food Delivery Driver'],
    'finance': ['Accountant', 'Financial Analyst', 'Investment Banker', 'Insurance Underwriter', 'Bank Teller'],
    'legal': ['Lawyer', 'Paralegal', 'Court Reporter', 'Judge', 'Legal Secretary', 'Claims Adjuster'],
    'emergency': ['Police Officer', 'Firefighter', 'EMT/Paramedic', 'Emergency Room Nurse', 'Air Traffic Controller', 'Dispatcher'],
    'creative': ['Graphic Designer', 'Writer', 'Marketing Specialist', 'Photographer', 'Film Director'],
    'transportation': ['Truck Driver', 'Taxi Driver', 'Delivery Driver', 'Airline Pilot'],
    'manufacturing': ['Factory Worker', 'Construction Worker', 'Electrician', 'Architect'],
    'energy': ['Oil Field Worker', 'Pipeline Engineer', 'Energy Analyst', 'Petroleum Engineer', 'Rig Operator'],
    'other': []
}

def get_job_categories():
    """Get available job categories"""
    return list(JOB_CATEGORIES.keys())

def get_jobs_by_category(category):
    """Get job titles in a specific category"""
    return JOB_CATEGORIES.get(category, [])

def add_custom_job(job_title, job_category=None):
    """
    Add a custom job to the JOB_DATA dictionary if it doesn't already exist
    
    Args:
        job_title (str): The job title to add
        job_category (str): The category for the job
        
    Returns:
        dict: The job data that was added or already existed
    """
    # Check if job already exists (case-insensitive)
    job_lower = job_title.lower()
    for known_job in JOB_DATA:
        if known_job.lower() == job_lower:
            return JOB_DATA[known_job]
    
    # If job doesn't exist, generate data based on similar jobs or create new data
    similar_jobs = []
    words_in_title = set(job_lower.split())
    
    # Find potentially similar jobs based on word overlap
    for known_job in JOB_DATA:
        known_job_words = set(known_job.lower().split())
        if words_in_title.intersection(known_job_words):
            similar_jobs.append(known_job)
    
    # Intelligently determine job category if not provided
    if not job_category:
        # Default to "other" category
        job_category = "other"
        
        # Management keywords
        if any(word in job_lower for word in ['manager', 'director', 'supervisor', 'chief', 'head', 'lead']):
            job_category = "management"
            
        # Technical keywords
        elif any(word in job_lower for word in ['engineer', 'developer', 'programmer', 'analyst', 'administrator', 'technician']):
            job_category = "technical"
            
        # Healthcare keywords
        elif any(word in job_lower for word in ['doctor', 'nurse', 'therapist', 'physician', 'medical', 'health', 'care']):
            job_category = "healthcare"
            
        # Education keywords
        elif any(word in job_lower for word in ['teacher', 'professor', 'instructor', 'educator', 'tutor', 'principal']):
            job_category = "education"
            
        # Service keywords
        elif any(word in job_lower for word in ['clerk', 'attendant', 'representative', 'cashier', 'associate', 'assistant']):
            job_category = "service"
            
        # Restaurant keywords
        elif any(word in job_lower for word in ['chef', 'cook', 'server', 'waiter', 'restaurant', 'food', 'kitchen']):
            job_category = "restaurant"
            
        # Finance keywords
        elif any(word in job_lower for word in ['accountant', 'auditor', 'finance', 'financial', 'banker', 'investment']):
            job_category = "finance"
            
        # Legal keywords
        elif any(word in job_lower for word in ['attorney', 'lawyer', 'legal', 'judge', 'paralegal', 'law']):
            job_category = "legal"
            
        # Emergency keywords
        elif any(word in job_lower for word in ['police', 'officer', 'firefighter', 'paramedic', 'emergency', 'security']):
            job_category = "emergency"
            
        # Creative keywords
        elif any(word in job_lower for word in ['designer', 'artist', 'writer', 'creative', 'media', 'producer']):
            job_category = "creative"
            
        # Transportation keywords
        elif any(word in job_lower for word in ['driver', 'pilot', 'conductor', 'logistics', 'transportation', 'delivery']):
            job_category = "transportation"
            
        # Manufacturing keywords
        elif any(word in job_lower for word in ['worker', 'operator', 'assembler', 'manufacturing', 'construction', 'fabricator', 'mechanic', 'maintenance']):
            job_category = "manufacturing"
            
        # Industry-specific adjustments - these override previous categorizations
        if "oil" in job_lower or "gas" in job_lower or "petroleum" in job_lower or "energy" in job_lower:
            job_category = "energy"
            
        # Add more industry-specific overrides as needed
    
    # Default risk values
    year_1_risk = 25.0
    year_5_risk = 45.0
    risk_level = "Moderate"
    risk_factors = {
        "Task Automation": 40,
        "AI Capability": 35,
        "Industry Adoption": 30,
        "Digital Transformation": 25,
        "Specialized Knowledge": 20
    }
    
    # If we found similar jobs, average their risk data
    if similar_jobs:
        total_year_1 = 0
        total_year_5 = 0
        total_jobs = len(similar_jobs)
        category_counts = {}
        
        for similar_job in similar_jobs:
            total_year_1 += JOB_DATA[similar_job]['year_1_risk']
            total_year_5 += JOB_DATA[similar_job]['year_5_risk']
            
            # Count job categories to find the most common one
            if 'job_category' in JOB_DATA[similar_job]:
                similar_category = JOB_DATA[similar_job]['job_category']
                if similar_category in category_counts:
                    category_counts[similar_category] += 1
                else:
                    category_counts[similar_category] = 1
        
        # Use most common category if we have counts and haven't already set one
        if category_counts and job_category == "other":
            most_common_category = max(category_counts.items(), key=lambda x: x[1])[0]
            job_category = most_common_category
        
        year_1_risk = total_year_1 / total_jobs
        year_5_risk = total_year_5 / total_jobs
        
        # Determine risk level based on 5-year risk
        if year_5_risk < 30:
            risk_level = "Low"
        elif year_5_risk < 50:
            risk_level = "Moderate" 
        elif year_5_risk < 70:
            risk_level = "High"
        else:
            risk_level = "Very High"
    
    # Create new job entry
    new_job_data = {
        'job_title': job_title,
        'year_1_risk': year_1_risk,
        'year_5_risk': year_5_risk,
        'risk_level': risk_level,
        'job_category': job_category,
        'risk_factors': risk_factors
    }
    
    # Add to JOB_DATA
    JOB_DATA[job_title] = new_job_data
    
    return new_job_data

def get_job_data(job_titles):
    """
    Get reliable job data for the specified job titles.
    If a job title is not in the database, it will be added with estimated data.
    
    Args:
        job_titles (list): List of job titles to analyze
        
    Returns:
        dict: Dictionary with job data
    """
    results = {}
    
    for job in job_titles:
        if not job:  # Skip empty job titles
            continue
            
        # Look for exact match
        if job in JOB_DATA:
            results[job] = JOB_DATA[job]
            continue
            
        # Try case-insensitive match
        job_lower = job.lower()
        found = False
        for known_job in JOB_DATA:
            if known_job.lower() == job_lower:
                # Copy data but preserve original job title
                data = JOB_DATA[known_job].copy()
                data['job_title'] = job  # Use the original case
                results[job] = data
                found = True
                break
        
        # If not found, add a custom job entry
        if not found:
            custom_job_data = add_custom_job(job)
            results[job] = custom_job_data
                
    return results

def create_comparison_table(job_data):
    """Create a pandas DataFrame for comparing jobs"""
    if not job_data:
        return pd.DataFrame()
        
    # Convert to list for DataFrame
    job_list = list(job_data.values())
    
    # Create DataFrame
    df = pd.DataFrame(job_list)
    
    # Ensure columns are in the right order
    columns = ['job_title', 'job_category', 'year_1_risk', 'year_5_risk', 'risk_level']
    
    # Make sure all columns exist
    for col in columns:
        if col not in df.columns:
            df[col] = 'N/A'
    
    df = df[columns]
    
    # Add numeric column for sorting
    df['sort_value'] = pd.to_numeric(df['year_5_risk'], errors='coerce')
    
    # Sort by year 5 risk (highest to lowest)
    df = df.sort_values(by='sort_value', ascending=False)
    
    # Drop the sorting column
    df = df.drop('sort_value', axis=1)
    
    # Format percentages
    df['year_1_risk'] = df['year_1_risk'].apply(lambda x: f"{float(x):.1f}%" if isinstance(x, (int, float)) else x)
    df['year_5_risk'] = df['year_5_risk'].apply(lambda x: f"{float(x):.1f}%" if isinstance(x, (int, float)) else x)
    
    return df

def create_comparison_chart(job_data):
    """Create a bar chart for comparing job risks"""
    if not job_data:
        return None
    
    # Extract job titles and risk values
    jobs = list(job_data.keys())
    
    # Get risk values
    year_1_values = []
    year_5_values = []
    
    for job in jobs:
        year_1_values.append(job_data[job]['year_1_risk'])
        year_5_values.append(job_data[job]['year_5_risk'])
    
    # Create the figure
    fig = go.Figure()
    
    # Add bars for 1-year risk
    fig.add_trace(go.Bar(
        x=jobs,
        y=year_1_values,
        name='1-Year Risk',
        marker_color='rgba(55, 83, 109, 0.7)',
        text=[f"{v:.1f}%" for v in year_1_values],
        textposition='auto',
    ))
    
    # Add bars for 5-year risk
    fig.add_trace(go.Bar(
        x=jobs,
        y=year_5_values,
        name='5-Year Risk',
        marker_color='rgba(26, 118, 255, 0.7)',
        text=[f"{v:.1f}%" for v in year_5_values],
        textposition='auto',
    ))
    
    # Customize layout
    fig.update_layout(
        title='AI Displacement Risk Comparison',
        xaxis_title='Job Title',
        yaxis_title='Risk Percentage',
        legend_title='Timeline',
        barmode='group',
        height=500
    )
    
    return fig

def create_risk_heatmap(job_data):
    """Create a heatmap for visualizing risk across job titles and time"""
    if not job_data:
        return None
    
    # Prepare data
    job_titles = list(job_data.keys())
    
    # Create data for the heatmap
    z_data = [[job_data[job]['year_1_risk'], job_data[job]['year_5_risk']] for job in job_titles]
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=z_data,
        x=['1-Year Risk', '5-Year Risk'],
        y=job_titles,
        colorscale='Viridis',
        colorbar=dict(title='Risk %'),
        text=[[f"{value:.1f}%" for value in row] for row in z_data],
        texttemplate="%{text}",
        textfont={"size":12}
    ))
    
    # Customize layout
    fig.update_layout(
        title='Risk Timeline Heatmap',
        xaxis_title='Timeline',
        yaxis_title='Job Title',
        height=500
    )
    
    return fig

def create_radar_chart(job_data):
    """Create a radar chart comparing job risk factors"""
    if not job_data or len(job_data) > 5:  # Limit to 5 jobs for readability
        return None
    
    # Create radar chart
    fig = go.Figure()
    
    # Add a trace for each job
    for job_title, job_info in job_data.items():
        # Calculate middle risk
        year_1 = job_info['year_1_risk']
        year_5 = job_info['year_5_risk']
        year_3 = (year_1 + year_5) / 2
        
        # Adaptability score (inverse of risk)
        adaptability = 100 - (year_5 * 0.8)
        
        # Skill transferability
        transferability = 100 - (year_5 * 0.6)
        
        # Add trace
        fig.add_trace(go.Scatterpolar(
            r=[year_1, year_5, year_3, adaptability, transferability],
            theta=['1-Year Risk', '5-Year Risk', '3-Year Risk', 'Adaptability', 'Skill Transferability'],
            fill='toself',
            name=job_title
        ))
    
    # Update layout
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )
        ),
        title='Job Risk Factor Comparison',
        showlegend=True,
        height=500
    )
    
    return fig

def create_factor_comparison(job_data):
    """Create a radar chart comparing specific risk factors"""
    if not job_data or len(job_data) > 3:  # Limit to 3 jobs for readability
        return None
        
    # Get all risk factors
    all_factors = set()
    for job_info in job_data.values():
        if 'risk_factors' in job_info:
            all_factors.update(job_info['risk_factors'].keys())
            
    if not all_factors:
        return None
        
    # Sort factors alphabetically
    factors = sorted(list(all_factors))
    
    # Create figure
    fig = go.Figure()
    
    # Add a trace for each job
    for job_title, job_info in job_data.items():
        if 'risk_factors' not in job_info:
            continue
            
        # Get values for each factor (default to 0 if not present)
        values = [job_info['risk_factors'].get(factor, 0) for factor in factors]
        
        # Add trace
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=factors,
            fill='toself',
            name=job_title
        ))
    
    # Update layout
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )
        ),
        title='Risk Factor Comparison by Job',
        showlegend=True,
        height=600
    )
    
    return fig