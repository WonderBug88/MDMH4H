# Data Mapping & Review - Map All Modal Feature

This branch introduces a new modal popup for the **Map All** functionality in our Data Mapping & Review application. The modal enables users to review the combined mapping attributes for available products before confirming the mapping. It uses Tailwind CSS for styling and vanilla JavaScript for DOM manipulation and AJAX requests.

## Features

- **Dynamic SKU Listing:**  
  The left-hand sidebar lists all parent products along with SKU availability. Only SKUs marked as "available" are used when mapping all products.

- **Map All Modal:**  
  Clicking the **Map All** button opens a modal popup displaying a dynamically generated table that shows:
  - SKU of available products.
  - Combined mapping details (Option 1 and, if provided, Option 2).
  
- **Confirm Mapping Action:**  
  The modal includes a **Confirm Mapping** button that sends a POST request to a simulated endpoint (`/map_all`) with the mapping data. (Currently, this endpoint simulates the mapping without writing to the database.)

- **Inline Editing:**  
  The Option Mapping & Review section uses a `contenteditable` div for the option names, allowing inline formatting.

- **Additional Functionalities:**  
  The branch retains the existing features such as attribute mapping, highlighting differences between uploaded and old option mappings, and adding custom attribute rows.

## Installation and Setup

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/yourrepo.git
   cd yourrepo
