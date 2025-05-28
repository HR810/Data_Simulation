import threading
import time
from datetime import datetime
from data_simulation import data_simulation
from productionplan_importer import import_productionplan

def main():
    print("Starting PPM Simulation Application")
    
    try:
        # Start simulation in a separate thread
        simulation_thread = threading.Thread(target=data_simulation, daemon=True)
        simulation_thread.start()
        print("Simulation thread started")
        
        last_run_date = None
        
        while True:
            try:
                now = datetime.now().date()
                
                if last_run_date != now:
                    print("Running daily production plan import")
                    import_productionplan()
                    last_run_date = now
                    print("Production plan import completed")
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                print(f"Error in main loop: {str(e)}")
                time.sleep(60)  # Wait before retrying
                
    except KeyboardInterrupt:
        print("Application stopped by user")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()